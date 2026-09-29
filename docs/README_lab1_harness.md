# Lab 1 test harness — COMP41720 (2026-27)

`lab1_harness.py` is the checker the demonstrator runs at your machine at the
end of the Lab 1 session. It is also yours: run it while you work, and arrive
already green.

## Requirements

Python 3.8+ only — no packages to install. It talks to your producer on
`http://localhost:8080` and to the RabbitMQ management API on
`http://localhost:15672` (both ports are mapped by the `docker run` command in
the lab brief).

## How to run it

```
python3 lab1_harness.py --quick      # while developing: contract + broker checks
python3 lab1_harness.py              # the full session check (interactive)
```

The full run walks the three criteria on the demonstrator's sheet:

1. **Contract green** — `POST /process` returns an id immediately;
   `GET /result/{id}` reports processing → completed; three concurrent
   requests get three distinct ids; the work queue is durable and a
   dead-letter queue is declared and bound.
2. **At-least-once, shown live** — the harness sends a request and you kill
   your consumer mid-processing. It watches the broker to confirm the message
   returned to the queue, you restart the consumer, and the same request must
   still complete.
3. **Poison message → DLQ** — you break your AI dependency (stop Ollama, or
   point the AI URL at a dead port), the harness sends one request and watches
   it land in your DLQ after your (finite!) retries are exhausted.

Useful flags: `--queue NAME` / `--dlq NAME` if auto-detection picks the wrong
queue, `--timeout SECONDS` if your model is slow to load, `--base URL` /
`--rabbit URL` for non-default ports.

A tip for criterion 2: the kill window is easiest to hit when processing is
slow — stop Ollama first so the AI call hangs, kill the consumer, then restart
both.

The harness never modifies your code or your broker; it only sends requests
and reads the management API. Exit code 0 means every check it ran passed.
