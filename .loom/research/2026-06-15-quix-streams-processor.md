Status: done
Created: 2026-06-15
Updated: 2026-06-15

# Quix Streams Processor Implementation Notes

## Question

Is Quix Streams viable for the Open Streaming Lab Python stream-processing chapter, and what API shape should the first processor use?

## Sources and methods

- Quix Streams quickstart: `https://quix.io/docs/quix-streams/quickstart.html`
  - Shows `Application(broker_address=..., consumer_group=...)`, `app.topic(..., value_deserializer="json")`, `app.dataframe(topic)`, `sdf.apply(...)`, and `app.run()`.
- Quix Streams Application API search result/docs:
  - Shows `Application.run(timeout=..., count=...)` stop conditions.
- Quix Streams configuration/docs search result:
  - Notes that stop conditions/callbacks can be used to stop after a number of processed messages.

## Findings

- Quix Streams is viable for the first processor because it supports JSON Kafka topics, DataFrame-like transformations, and bounded runs for local smoke testing.
- `Application.run(count=N, timeout=T)` can stop the processor after emitting records, which is important for automated validation.
- The first processor should be a deterministic one-to-one transform rather than a stateful/windowed aggregation. This keeps the chapter teachable and creates a derived topic useful for the later DuckDB sink.

## Conclusions

Use Quix Streams for `kafka-process`: consume valid Web Analytics JSON records, transform them to `pageview_by_url` events, key output by `page_url`, write to `mockingbird.pageviews.by_url`, and use `--max-outputs`/`--timeout` for finite smoke tests.

## Related records

- `.loom/tickets/done/2026-06-15-python-stream-processor.md`
- `.loom/decisions/python-first-streaming-roadmap.md`
