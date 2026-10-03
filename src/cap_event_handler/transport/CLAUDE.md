# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## The seam

`base.Transport` is a `Protocol` and describes everything the library asks of a broker. Topic and
queue ids are opaque strings: ARNs and queue URLs on AWS, plain names in `MemoryTransport`.
There are two implementations, and **both must change together**. A new method or a change of
semantics goes into `base.py`, `memory.py` and `snssqs.py` in the same change.

Contract points the rest of the library relies on:

- `ack` is called only after the handler's transaction commits. `reject` releases the lease
  (sets visibility) instead of deleting the message. Redelivery, backoff and dead-lettering come
  from the broker, not from the library.
- `ReceivedMessage.receive_count` counts this delivery too (SQS `ApproximateReceiveCount`).
- `queue_attributes()` feeds startup validation (`validation.py`): visibility timeout,
  retention, whether a redrive policy exists, and which topics are subscribed.

## `memory.py`: a broker simulator, not a mock

It models visibility leases, receive counts and redrive to `dead_letter` (enabled by
`max_receive_count`), driven by the injected clock. To test a timeout, advance the clock rather
than sleeping: `receive()` ignores `wait_seconds`. Test hooks:

- `sent`: every publish.
- `fail_publish`: set an exception to make `publish` raise.
- `dead_letter`: messages that hit the redrive limit.

When the real SQS behaviour you depend on isn't modelled here yet, extend the simulator to match
SQS. Don't special-case the code under test.

## `snssqs.py`

- It uses aiobotocore, not boto3, on purpose. Each long poll holds a connection for up to 20
  seconds, and boto3 in a thread pool would pin one thread per poll. Clients are created lazily
  and owned by an `AsyncExitStack`, and `close()` tears them down.
- SNS→SQS delivery wraps the payload in a notification document. `unwrap_notification` extracts
  `Message`, and headers come from `MessageAttributes`. Raw message delivery skips the wrapper,
  so a body that isn't a notification is passed through, and its headers are read from the SQS
  message attributes instead. Keep both paths working.
- `provision=False` never creates anything. A missing topic or queue raises `ConfigurationError`,
  whose message names `provision=False`, and `subscribe` does nothing (Terraform owns the wiring).
- `subscribe` **overwrites** the queue's access policy with one statement per topic.
- `SnsSqs(...)` is the public constructor-style factory. Its camel-case name is deliberate
  (`noqa: N802`).

Unit coverage for the encode/decode/unwrap helpers is in `tests/test_snssqs_unit.py`. Real
behaviour against AWS is tested only in `tests/integration/test_localstack.py`.
