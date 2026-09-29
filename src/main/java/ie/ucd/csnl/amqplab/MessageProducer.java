package ie.ucd.csnl.amqplab;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import tools.jackson.databind.json.JsonMapper;

@RestController
public class MessageProducer {

    private final RabbitTemplate rabbitTemplate;
    private final JobStore jobStore;
    private final JsonMapper jsonMapper;
    private final MessageDeliveryMode deliveryMode;
    // Sequential ids (1, 2, 3, ...), continuing from the highest id in the results file.
    private final AtomicLong nextId;

    public MessageProducer(RabbitTemplate rabbitTemplate, JobStore jobStore, JsonMapper jsonMapper,
                           @Value("${producer.persistent-messages}") boolean persistent) {
        this.rabbitTemplate = rabbitTemplate;
        this.jobStore = jobStore;
        this.jsonMapper = jsonMapper;
        this.nextId = new AtomicLong(jobStore.maxId() + 1);
        // PERSISTENT (delivery mode 2) = broker writes the message to disk so it survives a broker restart.
        this.deliveryMode = persistent ? MessageDeliveryMode.PERSISTENT : MessageDeliveryMode.NON_PERSISTENT;
    }

    /** Request body clients send: {"text": "Process this request"} */
    public record ProcessRequest(String text) {
    }

    /** Accepts a request, enqueues it, and returns the request id immediately (no waiting for the AI). */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> process(@RequestBody ProcessRequest request) {
        if (request.text() == null || request.text().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "\"text\" is required"));
        }
        AiJob job = new AiJob(nextId.getAndIncrement(), request.text(), Instant.now().truncatedTo(ChronoUnit.SECONDS));
        jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.PROCESSING, job.text(), null, null, 0));
        // Plain JSON body ({"id":..,"text":..,"timestamp":..}), no Java type headers, so any client can read it.
        Message message = MessageBuilder.withBody(jsonMapper.writeValueAsBytes(job))
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .setDeliveryMode(deliveryMode)
                .build();
        rabbitTemplate.send(RabbitConfig.EXCHANGE, RabbitConfig.ROUTING_KEY, message);
        return ResponseEntity.accepted().body(Map.of("id", job.id(), "status", JobStore.Status.PROCESSING));
    }

    /** Returns the stored AI response, or the status: "processing", "completed" or "error". */
    @GetMapping("/result/{id}")
    public ResponseEntity<?> result(@PathVariable long id) {
        return jobStore.get(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("id", id, "error", "unknown request id")));
    }
}
