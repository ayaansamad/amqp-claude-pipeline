package ie.ucd.csnl.amqplab;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * In-memory job status store shared by producer and consumer.
 * Fine for a single-process lab; a real deployment would use Redis/a DB or a reply queue.
 */
@Component
public class JobStore {

    public enum Status { QUEUED, PROCESSING, DONE, FAILED }

    public record JobResult(long id, Status status, String text, String answer, String error) {
    }

    private final Map<Long, JobResult> jobs = new ConcurrentHashMap<>();

    public void put(JobResult result) {
        jobs.put(result.id(), result);
    }

    public Optional<JobResult> get(long id) {
        return Optional.ofNullable(jobs.get(id));
    }
}
