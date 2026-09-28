package ie.ucd.csnl.amqplab;

import java.time.Instant;

/**
 * The message that travels through the queue, serialized as JSON:
 * {"id": 1, "text": "Process this request", "timestamp": "2026-10-05T12:00:00Z"}
 */
public record AiJob(long id, String text, Instant timestamp) {
}
