package ie.ucd.csnl.amqplab;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import tools.jackson.databind.json.JsonMapper;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    /** Header counting how many times a message has been sent back for another attempt. */
    static final String RETRY_COUNT_HEADER = "x-retry-count";

    private final OllamaService ai;
    private final JobStore jobStore;
    private final RabbitTemplate rabbitTemplate;
    private final JsonMapper jsonMapper;
    private final AcknowledgeMode ackMode;
    private final boolean crashMidProcessing;
    private final int maxRetries;
    private final long retryDelayMs;

    // Rate limit shared by all listener threads: at most one message starts per interval.
    private final long intervalNanos;
    private final Object rateLock = new Object();
    private long nextSlotNanos = System.nanoTime();

    public MessageConsumer(OllamaService ai, JobStore jobStore, RabbitTemplate rabbitTemplate, JsonMapper jsonMapper,
                           @Value("${consumer.messages-per-second}") double messagesPerSecond,
                           @Value("${spring.rabbitmq.listener.simple.acknowledge-mode}") AcknowledgeMode ackMode,
                           @Value("${consumer.crash-mid-processing}") boolean crashMidProcessing,
                           @Value("${consumer.max-retries}") int maxRetries,
                           @Value("${consumer.retry-delay-ms}") long retryDelayMs) {
        this.ai = ai;
        this.jobStore = jobStore;
        this.rabbitTemplate = rabbitTemplate;
        this.jsonMapper = jsonMapper;
        this.intervalNanos = messagesPerSecond > 0 ? (long) (1_000_000_000L / messagesPerSecond) : 0;
        this.ackMode = ackMode;
        this.crashMidProcessing = crashMidProcessing;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        log.info("Consumer started: acknowledge-mode={}, max-retries={}, retry-delay={}ms, crash-mid-processing={}",
                ackMode, maxRetries, retryDelayMs, crashMidProcessing);
    }

    @RabbitListener(queues = RabbitConfig.QUEUE)
    public void receive(Message message, Channel channel) throws InterruptedException, IOException {
        MessageProperties props = message.getMessageProperties();
        long deliveryTag = props.getDeliveryTag();
        Integer retryHeader = props.getHeader(RETRY_COUNT_HEADER);
        int retries = retryHeader == null ? 0 : retryHeader;

        // A message we can't even parse will never succeed: dead-letter it straight away.
        AiJob job;
        try {
            job = jsonMapper.readValue(message.getBody(), AiJob.class);
            if (job.text() == null) {
                throw new IllegalArgumentException("missing \"text\"");
            }
        } catch (Exception e) {
            log.error("Poison message (cannot parse: {}), sending to DLQ: {}", e.getMessage(), new String(message.getBody()));
            deadLetter(message, channel, deliveryTag);
            return;
        }

        awaitRateLimit();

        Instant processingAt = Instant.now();
        log.info("Processing message id={} text=\"{}\" processingTimestamp={} redelivered={} attempt={}/{} (queued at {}, waited {} ms)",
                job.id(), job.text(), processingAt, props.isRedelivered(), retries + 1, maxRetries + 1, job.timestamp(),
                job.timestamp() == null ? "?" : Duration.between(job.timestamp(), processingAt).toMillis());

        if (crashMidProcessing) {
            // Simulate a hard crash (like kill -9): no ack, no shutdown hooks, no clean channel close.
            log.error("Simulating crash while processing message id={} (not acknowledged)", job.id());
            Runtime.getRuntime().halt(1);
        }

        try {
            String answer = ai.ask(job.text());
            jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.COMPLETED, job.text(), answer, null, retries + 1));
            ack(channel, deliveryTag);
            log.info("Job {} completed, acknowledged", job.id());
        } catch (Exception e) {
            String reason = e.getClass().getSimpleName() + ": " + e.getMessage();
            if (OllamaService.isTransient(e) && retries < maxRetries) {
                // Transient failure: park a copy in the delay queue, then ack the original.
                // (Publish before ack: if we crash in between, the worst case is a duplicate, never a loss.)
                log.warn("Job {} attempt {}/{} failed ({}); retrying in {} ms",
                        job.id(), retries + 1, maxRetries + 1, reason, retryDelayMs);
                jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.PROCESSING, job.text(), null, reason, retries + 1));
                scheduleRetry(message, retries + 1);
                ack(channel, deliveryTag);
            } else {
                String why = OllamaService.isTransient(e) ? "retries exhausted" : "permanent error";
                log.error("Job {} failed ({}: {}); sending to DLQ", job.id(), why, reason);
                jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.ERROR, job.text(), null, reason, retries + 1));
                deadLetter(message, channel, deliveryTag);
            }
        }
    }

    /** Re-publish to the delay queue; when its TTL expires the broker routes it back to task_queue. */
    private void scheduleRetry(Message original, int retryCount) {
        MessageProperties props = original.getMessageProperties();
        props.setHeader(RETRY_COUNT_HEADER, retryCount);
        props.setExpiration(String.valueOf(retryDelayMs));
        MessageDeliveryMode mode = props.getReceivedDeliveryMode();
        props.setDeliveryMode(mode != null ? mode : MessageDeliveryMode.PERSISTENT);
        rabbitTemplate.send("", RabbitConfig.RETRY_QUEUE, original);
    }

    private void ack(Channel channel, long deliveryTag) throws IOException {
        // In MANUAL mode the broker keeps the message (unacked) until we get here; a crash before this
        // line makes RabbitMQ redeliver it. In NONE (auto-ack) mode it was removed on delivery.
        if (ackMode == AcknowledgeMode.MANUAL) {
            channel.basicAck(deliveryTag, false);
        }
    }

    /**
     * Reject without requeue: task_queue's x-dead-letter-exchange makes the broker route the message
     * to lab.dlx, and on to task_queue.dlq. (With auto-ack there is nothing to reject, so publish there directly.)
     */
    private void deadLetter(Message message, Channel channel, long deliveryTag) throws IOException {
        if (ackMode == AcknowledgeMode.MANUAL) {
            channel.basicReject(deliveryTag, false);
        } else {
            rabbitTemplate.send(RabbitConfig.DEAD_LETTER_EXCHANGE, RabbitConfig.DEAD_LETTER_ROUTING_KEY, message);
        }
    }

    /** Blocks until this thread's turn: claims the next free slot, then sleeps until it arrives. */
    private void awaitRateLimit() throws InterruptedException {
        if (intervalNanos == 0) {
            return;
        }
        long waitNanos;
        synchronized (rateLock) {
            long now = System.nanoTime();
            long slot = Math.max(now, nextSlotNanos);
            nextSlotNanos = slot + intervalNanos;
            waitNanos = slot - now;
        }
        if (waitNanos > 0) {
            Thread.sleep(Duration.ofNanos(waitNanos));
        }
    }
}
