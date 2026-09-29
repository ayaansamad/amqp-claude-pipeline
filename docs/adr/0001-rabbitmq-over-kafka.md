```
ARCHITECTURAL DECISION RECORD (ADR)
------------------------------------
Title:  Use RabbitMQ (not Kafka) as the broker for the async AI processing service
Date:   2026-09-28
Status: Accepted
```

## Context

The service accepts text on `POST /process` and returns a request id straight away. The text is then sent to an AI model (a local LLM served by Ollama, which could equally be a cloud API such as OpenAI or Anthropic), and the client polls `GET /result/{id}` for the outcome. These forces shape the choice of broker:

- **The work is a set of independent tasks, not an event stream.** Each message is one unit of work that one consumer should handle once and then discard. No other service needs to read it, and nothing needs to replay history.
- **The downstream API is slow and unreliable.** A model call takes seconds, the server can be down, restarting or loading the model, and cloud APIs add rate limits (429) and 5xx errors on top. A failed task has to be retried after a delay, a fixed number of times. After that it has to be set aside without blocking the tasks behind it (the retry and DLQ requirements).
- **Per-message acknowledgement.** The consumer must ack each message only after processing it (at-least-once delivery), so that a crash mid-call leads to redelivery (assessment criterion 2).
- **Throughput is low**, from a handful to hundreds of requests per minute, and is limited by the AI API rather than the broker. The load is bursty.
- **Small team, lab timeframe.** The system runs on one laptop using Docker and Spring Boot, and both team members must be able to explain every part of it.

## Decision

Use **RabbitMQ** (classic durable queues, AMQP 0-9-1 via Spring AMQP) as a work queue:

- `lab.exchange` (direct) routes to the durable `task_queue`. Messages are published as persistent.
- The consumer uses manual acks with `prefetch=1`, so each thread holds one unacked task at a time and parallel work is spread evenly across threads.
- Transient failures are re-published to `task_queue.retry`, where a per-message TTL of 5 s dead-letters them back to `task_queue`. After 3 retries, or on a permanent error, the consumer rejects the message, and `task_queue`'s `x-dead-letter-exchange` (`lab.dlx`) routes it to `task_queue.dlq`.

## Consequences

**Positive**
- Per-message ack, reject and redelivery come from the protocol itself. A message that fails is handled on its own; it doesn't hold up or rewind anything else.
- Dead-lettering, TTL and delayed retry are all broker features, configured by queue arguments. Implementing them needed about 60 lines of Spring configuration and no extra infrastructure.
- Competing consumers scale horizontally: start another instance and RabbitMQ shares tasks between them. Prefetch keeps a slow AI call from causing a pile-up.
- The single `rabbitmq:4-management` container includes a UI where queue depth, the retry queue and the DLQ can be watched live during the demo.

**Negative**
- Once acked, a message is gone. There is no way to replay past requests, for example to reprocess them with a new model or prompt. If that becomes a requirement, the requests would need to be stored separately.
- Ordering isn't guaranteed with several consumers and retries. That's acceptable here because each task is independent.
- The throughput ceiling is lower than Kafka's, which is irrelevant at this load because the AI API is the bottleneck by orders of magnitude.
- Durability depends on the broker's data volume (added to `compose.yaml`). A single node is a single point of failure; production would need a cluster with quorum queues.

## Alternatives considered

- **Apache Kafka.** Kafka is built for high-throughput, replayable event streams, with ordering per partition. It fits badly here:
  - It has no per-message ack or native DLQ. A consumer commits an offset, so one poison message blocks its whole partition unless we write our own retry topics and DLQ topic.
  - It has no per-message TTL or delayed redelivery, so the back-off would need separate retry topics and consumers.
  - Parallelism is capped by the number of partitions, not by how many consumers you start.
  - It needs more infrastructure to run: a KRaft or ZooKeeper cluster and a topic/partition design.

  Kafka's advantages (replay, very high throughput, many independent consumer groups) are not needed by this use case.
- **A cloud queue (AWS SQS with a redrive policy).** SQS has similar semantics, including visibility timeouts and a built-in DLQ. But it needs a cloud account and network access, and it can't run locally in the lab.
- **No broker (call the AI synchronously, or use an in-process thread pool).** This loses the decoupling the lab is about. Queued requests would disappear on a crash, and there would be nowhere to hold failures.
