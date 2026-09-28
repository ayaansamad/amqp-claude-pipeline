package ie.ucd.csnl.amqplab;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageProducer {

    private final RabbitTemplate rabbitTemplate;
    private final JobStore jobStore;
    // Sequential ids (1, 2, 3, ...); resets when the app restarts.
    private final AtomicLong nextId = new AtomicLong(1);

    public MessageProducer(RabbitTemplate rabbitTemplate, JobStore jobStore) {
        this.rabbitTemplate = rabbitTemplate;
        this.jobStore = jobStore;
    }

    /** Request body clients send: {"text": "Process this request"} */
    public record SubmitRequest(String text) {
    }

    /** Accepts a prompt, enqueues it, and returns immediately with a job id to poll. */
    @PostMapping("/jobs")
    public ResponseEntity<Map<String, Object>> submit(@RequestBody SubmitRequest request) {
        if (request.text() == null || request.text().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "\"text\" is required"));
        }
        AiJob job = new AiJob(nextId.getAndIncrement(), request.text(), Instant.now().truncatedTo(ChronoUnit.SECONDS));
        jobStore.put(new JobStore.JobResult(job.id(), JobStore.Status.QUEUED, job.text(), null, null));
        rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE, RabbitConfig.ROUTING_KEY, job);
        return ResponseEntity.accepted().body(Map.of("id", job.id()));
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<JobStore.JobResult> status(@PathVariable long id) {
        return ResponseEntity.of(jobStore.get(id));
    }
}
