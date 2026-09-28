package ie.ucd.csnl.amqplab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final ClaudeService claude;
    private final JobStore jobStore;

    public MessageConsumer(ClaudeService claude, JobStore jobStore) {
        this.claude = claude;
        this.jobStore = jobStore;
    }

    @RabbitListener(queues = RabbitConfig.QUEUE)
    public void receive(AiJob job) {
        log.info("Processing job {}", job.id());
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
}
