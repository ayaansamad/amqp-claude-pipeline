# AMQP Claude Pipeline

A small Spring Boot app showing asynchronous job processing over RabbitMQ (AMQP).
A producer accepts requests over HTTP and enqueues them. A consumer takes them off the queue and processes each one by calling the Claude API.

```
            POST /jobs                                   ┌──────────────┐
 client ───────────────► MessageProducer ──► lab.exchange ──► task_queue ──► MessageConsumer ──► Claude API
   ▲       202 {"id":1}        │                (direct)          │
   │                           ▼                                  ▼
   └──── GET /jobs/{id} ◄── JobStore ◄──────────────── status + answer
```

## How it works

1. The client sends `{"text": "..."}` to `POST /jobs`.
2. The producer assigns an `id` and a `timestamp`, records the job as `QUEUED`, publishes it to `lab.exchange` (routing key `lab.key`), and returns `202 Accepted` straight away.
3. The consumer (`@RabbitListener` on `task_queue`) receives the message, marks the job `PROCESSING`, and sends `text` to Claude.
4. The result is stored as `DONE` with the answer, or as `FAILED` with the error. The client polls `GET /jobs/{id}` for it.

Every message on the queue is JSON with this shape:

```json
{"id": 1, "text": "Process this request", "timestamp": "2026-10-05T12:00:00Z"}
```

## Project structure

| File | Role |
|---|---|
| `MessageProducer.java` | REST endpoints: `POST /jobs` enqueues a job, `GET /jobs/{id}` returns its status |
| `MessageConsumer.java` | Queue listener: processes each job and records the outcome |
| `ClaudeService.java` | Calls the Claude API with the official Anthropic Java SDK |
| `AiJob.java` | The queue message (`id`, `text`, `timestamp`) |
| `JobStore.java` | In-memory job status store |
| `RabbitConfig.java` | Declares the exchange, the queue, the binding, and the JSON message converter |

## Prerequisites

- Java 21+
- Docker (to run RabbitMQ)
- An Anthropic API key ([console.anthropic.com](https://console.anthropic.com))

## Running

Start RabbitMQ:

```bash
docker compose up -d
```

Start the app with your API key set:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
./mvnw spring-boot:run
```

Submit a job:

```bash
curl -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"text": "Explain AMQP in one sentence"}'
# {"id":1}
```

Poll for the result:

```bash
curl localhost:8080/jobs/1
# {"id":1,"status":"DONE","text":"Explain AMQP in one sentence","answer":"...","error":null}
```

A job's status moves from `QUEUED` to `PROCESSING`, then ends as `DONE` or `FAILED`.

To watch messages flow through the queue, open the RabbitMQ management UI at <http://localhost:15672> (login `guest` / `guest`).

To watch the queue drain at the controlled rate, submit a batch of jobs and check the live queue depth:

```bash
for i in $(seq 1 20); do
  curl -s -X POST localhost:8080/jobs -H 'Content-Type: application/json' -d "{\"text\":\"Request $i\"}"
done
docker exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

The consumer logs every message it starts, for example:

```
Processing message id=3 text="Request 3" processingTimestamp=2026-09-28T11:54:10.967Z (queued at 2026-09-28T11:54:08Z, waited 2967 ms)
```

## API

| Method | Path | Body | Response |
|---|---|---|---|
| `POST` | `/jobs` | `{"text": "..."}` | `202` `{"id": 1}`. `400` if `text` is missing or blank. `415` if the body isn't JSON. |
| `GET` | `/jobs/{id}` | none | `200` with the job status. `404` if the id is unknown. |

## Configuration

Set in `src/main/resources/application.properties`:

| Property | Default | Meaning |
|---|---|---|
| `claude.model` | `claude-opus-5` | Claude model used by the consumer |
| `claude.max-tokens` | `16000` | Maximum length of each answer |
| `consumer.messages-per-second` | `1` | Rate limit shared by all consumer threads: at most this many messages start per second (`0` = unlimited) |
| `spring.rabbitmq.listener.simple.concurrency` / `max-concurrency` | `4` / `4` | Number of consumer threads processing jobs in parallel |
| `spring.rabbitmq.listener.simple.prefetch` | `1` | Messages each consumer thread takes from the queue at a time |
| `spring.rabbitmq.listener.simple.default-requeue-rejected` | `false` | Failed messages are not requeued, which prevents infinite redelivery |

## Error handling

- The Anthropic SDK retries rate limits (429), server errors (5xx), and connection failures by itself.
- Any error still left after those retries marks the job `FAILED`, and the error message is stored with the job.
- If Claude's safety checks refuse a request, the API retries it on a fallback model (the `server-side-fallback` beta). If the fallback also refuses, the job is marked `FAILED`.

## Limitations

- **Job status is kept in memory.** It is lost when the app restarts and can't be shared between separate producer and consumer processes. A real deployment would use Redis or a database, or send results back on a reply queue.
- **Job ids restart at 1** when the app restarts.
- **Failed jobs are dropped from the queue** instead of being sent to a dead-letter queue.

## Tests

Start RabbitMQ first (`docker compose up -d`), then run:

```bash
./mvnw test
```
