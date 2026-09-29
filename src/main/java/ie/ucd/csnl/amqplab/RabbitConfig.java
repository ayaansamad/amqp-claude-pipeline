package ie.ucd.csnl.amqplab;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topology:
 * <pre>
 *  producer ─► lab.exchange ──(lab.key)──► task_queue ─► consumer
 *                  ▲                          │  reject (no requeue)
 *                  │ TTL expires              ▼
 *        task_queue.retry ◄── consumer    lab.dlx ──(task_queue.dlq)──► task_queue.dlq
 *        (transient failure, retry < max)
 * </pre>
 */
@Configuration
public class RabbitConfig {

    public static final String EXCHANGE = "lab.exchange";
    public static final String QUEUE = "task_queue";
    public static final String ROUTING_KEY = "lab.key";

    public static final String RETRY_QUEUE = "task_queue.retry";

    public static final String DEAD_LETTER_EXCHANGE = "lab.dlx";
    public static final String DEAD_LETTER_QUEUE = "task_queue.dlq";
    public static final String DEAD_LETTER_ROUTING_KEY = "task_queue.dlq";

    /**
     * Durable main queue. Messages the consumer rejects (basicReject, requeue=false) are
     * dead-lettered by the broker to lab.dlx, which routes them to task_queue.dlq.
     */
    @Bean
    Queue queue() {
        return QueueBuilder.durable(QUEUE)
                .deadLetterExchange(DEAD_LETTER_EXCHANGE)
                .deadLetterRoutingKey(DEAD_LETTER_ROUTING_KEY)
                .build();
    }

    /** Durable exchange (durable=true, autoDelete=false), so the binding survives a broker restart too. */
    @Bean
    DirectExchange exchange() {
        return new DirectExchange(EXCHANGE, true, false);
    }

    @Bean
    Binding binding() {
        return BindingBuilder.bind(queue()).to(exchange()).with(ROUTING_KEY);
    }

    /**
     * Delay queue for retries. Nothing consumes it: each message carries a per-message TTL
     * (expiration = consumer.retry-delay-ms), and when it expires the broker dead-letters it
     * back to lab.exchange / lab.key, i.e. onto task_queue for another attempt.
     */
    @Bean
    Queue retryQueue() {
        return QueueBuilder.durable(RETRY_QUEUE)
                .deadLetterExchange(EXCHANGE)
                .deadLetterRoutingKey(ROUTING_KEY)
                .build();
    }

    /** Dead-letter exchange: receives messages that failed permanently or ran out of retries. */
    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(DEAD_LETTER_EXCHANGE, true, false);
    }

    /** Dead-letter queue: failed messages park here for inspection or manual replay. */
    @Bean
    Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
    }

    @Bean
    Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange()).with(DEAD_LETTER_ROUTING_KEY);
    }
}
