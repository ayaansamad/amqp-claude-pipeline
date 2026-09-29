package ie.ucd.csnl.amqplab;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonValue;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Results store, keyed by request id. Kept in memory and mirrored to a JSON file, so results
 * (and the id counter) survive an app restart, e.g. when demonstrating redelivery after a crash.
 */
@Component
public class JobStore {

    private static final Logger log = LoggerFactory.getLogger(JobStore.class);

    public enum Status {
        PROCESSING, COMPLETED, ERROR;

        /** Serialized as "processing" / "completed" / "error". */
        @JsonValue
        public String json() {
            return name().toLowerCase();
        }
    }

    public record JobResult(long id, Status status, String text, String result, String error, int attempts) {
    }

    private final Map<Long, JobResult> jobs = new ConcurrentHashMap<>();
    private final JsonMapper jsonMapper;
    private final Path file;

    public JobStore(JsonMapper jsonMapper, @Value("${results.file}") String file) {
        this.jsonMapper = jsonMapper;
        this.file = Path.of(file);
        if (Files.exists(this.file)) {
            List<JobResult> saved = jsonMapper.readValue(this.file.toFile(), new TypeReference<List<JobResult>>() {});
            saved.forEach(r -> jobs.put(r.id(), r));
            log.info("Loaded {} results from {}", saved.size(), this.file.toAbsolutePath());
        }
    }

    public synchronized void put(JobResult result) {
        jobs.put(result.id(), result);
        save();
    }

    public Optional<JobResult> get(long id) {
        return Optional.ofNullable(jobs.get(id));
    }

    public long maxId() {
        return jobs.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
    }

    /** Write to a temp file, then rename, so a crash never leaves a half-written results file. */
    private void save() {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            jsonMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), new ArrayList<>(jobs.values()));
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            log.error("Could not save results to {}", file, e);
        }
    }
}
