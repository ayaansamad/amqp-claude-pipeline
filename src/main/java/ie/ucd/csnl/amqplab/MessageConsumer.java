package ie.ucd.csnl.amqplab;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final ClaudeService claude;
    private final JobStore jobStore;

    // Rate limit shared by all listener threads: at most one message starts per interval.
    private final long intervalNanos;
    private final Object rateLock = new Object();
    private long nextSlotNanos = System.nanoTime();

    public MessageConsumer(ClaudeService claude, JobStore jobStore,
                           @Value("${consumer.messages-per-second}") double messagesPerSecond) {
        this.claude = claude;
        this.jobStore = jobStore;
        this.intervalNanos = messagesPerSecond > 0 ? (long) (1_000_000_000L / messagesPerSecond) : 0;
    }

    @RabbitListener(queues = RabbitConfig.QUEUE)
    public void receive(AiJob job) throws InterruptedException {
        awaitRateLimit();

        Instant processingAt = Instant.now();
        log.info("Processing message id={} text=\"{}\" processingTimestamp={} (queued at {}, waited {} ms)",
                job.id(), job.text(), processingAt, job.timestamp(),
                Duration.between(job.timestamp(), processingAt).toMillis());

        jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.PROCESSING, job.text(), null, null));
        try {
            String answer = claude.ask(job.text());
            jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.DONE, job.text(), answer, null));
            log.info("Job {} done", job.id());
        } catch (Exception e) {
            // The SDK already retries 429/5xx/connection errors; anything reaching here is final.
            log.error("Job {} failed", job.id(), e);
            jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.FAILED, job.text(), null, e.getMessage()));
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
