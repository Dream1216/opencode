# OpenCode v1.18.8 Upgrade Review

P5.1.1 reviews the only protected upstream change detected between the pinned
OpenCode v1.18.7 baseline and v1.18.8. It does not merge or activate the
candidate release.

## Reviewed change

`packages/opencode/src/session/tools.ts` changes one MCP catalog call:

```diff
- const item = McpCatalog.convertTool(entry.def, entry.client, entry.timeout)
+ const item = McpCatalog.convertTool(entry)
```

The change adapts the call to the v1.18.8 `McpCatalog.convertTool` entry
signature. It does not move or bypass `tool.execute.before`,
`tool.execute.after`, permission admission, the VibeCode policy adapter, or
worker fencing.

The compatibility resolution is fail-closed and bound to these exact files:

- v1.18.7 SHA-256:
  `ec76a577f6f68d44c6202dcd3fae1d3b859c8a3bda603939364048ce033be095`
- v1.18.8 SHA-256:
  `9020dac499b135086258007c5352d94327cee75f624a7694b4d1156c82f0710c`

Any new candidate content or tag mismatch returns the path to manual review.

## No-merge rehearsal

`packages/core/script/upstream-v1-18-8-rehearsal.ts`:

1. verifies the pinned baseline, candidate, and local file hashes;
2. rejects overlap between official candidate source/test changes and local
   modifications;
3. copies the local checkout to a disposable directory;
4. overlays only official v1.18.8 source/test changes;
5. runs core, schema, and OpenCode typechecks, the session suite, official tool
   tests, shadow-agent-run smoke, and tool-governance smoke;
6. deletes the disposable checkout unless `OPENCODE_KEEP_REHEARSAL=1`.

This produces upgrade evidence without changing the working tree or declaring
v1.18.8 deployed.
