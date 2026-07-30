# ModelInvocationGateway

`ModelInvocationGateway` wraps the session runner's provider stream boundary without changing provider implementations.

Current P1 behavior:

- Publishes `session.next.model.invocation.attempted` before each provider stream attempt.
- Publishes `session.next.model.invocation.usage` whenever an LLM stream event carries usage.
- Publishes `session.next.model.invocation.failed` for provider-error events or stream failures.
- Publishes `session.next.model.invocation.completed` after a successful provider stream.
- Exposes `retry()` and `session.next.model.invocation.retried` as the retry event boundary, but does not enable automatic retry yet.
- Keeps cost calculation intentionally conservative at `0 USD` until catalog pricing is connected.

This preserves OpenCode's existing overflow compaction, continuation, and tool settlement semantics while creating one canonical model-invocation observation layer.
