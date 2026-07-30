# Session workflow mapping

P3 maps complex prompts to a VibeCode-style workflow definition/version without changing OpenCode's main runner loop.

The sidecar emits `session.next.workflow.bound` after prompt admission when the prompt is classified as complex. The bound workflow is a JSON configuration source with `nodes`, `edges`, and `settings`; it is not compiled to LangGraph and does not route execution yet.

Current default version:

- `definition.id`: `wf_vibecode_complex_task_default`
- `version.id`: `wfv_vibecode_complex_task_default_1`
- `runtime`: `opencode-main-loop-sidecar`

This keeps workflow definition/version persistence and replay separate from execution. A later phase can compile the same definition JSON to a graph runtime behind a feature flag.
