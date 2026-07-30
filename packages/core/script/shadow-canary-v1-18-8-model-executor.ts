import { createHash } from "node:crypto"
import { spawn } from "node:child_process"
import { mkdir, readFile } from "node:fs/promises"
import { join, resolve } from "node:path"
import type { ShadowCanaryOutcome } from "../src/session/runner/shadow-canary"

const started = Date.now()
const body = await new Response(Bun.stdin.stream()).text()
const envelope = JSON.parse(body) as Record<string, unknown>
const invocation = recordValue(envelope.invocation)
const request = recordValue(envelope.request)
const requestDigest = required("OPENCODE_SHADOW_REQUEST_DIGEST")
const invocationID = required("OPENCODE_SHADOW_INVOCATION_ID")
const checkout = resolve(required("OPENCODE_SHADOW_CANDIDATE_CHECKOUT"))
const manifest = JSON.parse(await readFile(join(checkout, "packages/opencode/package.json"), "utf8")) as {
  version?: unknown
}
if (manifest.version !== "1.18.8") {
  throw new Error(`candidate checkout must be OpenCode v1.18.8, received ${String(manifest.version)}`)
}
if (envelope.version !== "opencode-shadow-canary-request.v1" || envelope.shadowOnly !== true) {
  throw new Error("invalid shadow canary request envelope")
}
if (invocation.id !== invocationID) throw new Error("candidate invocation correlation mismatch")

const model = recordValue(invocation.model)
const providerID = text(model.providerID)
const modelID = text(model.modelID)
if (!providerID || !modelID) throw new Error("candidate request model reference is missing")
const prompt = latestUserPrompt(request)
if (!prompt) throw new Error("candidate request does not contain a user prompt")

const base = parseObject(process.env.OPENCODE_SHADOW_CANDIDATE_CONFIG_CONTENT ?? "{}")
const baseAgents = recordValue(base.agent)
const candidateAgent = recordValue(baseAgents["shadow-canary"])
const config = {
  ...base,
  formatter: false,
  lsp: false,
  permission: { ...recordValue(base.permission), "*": "deny" },
  agent: {
    ...baseAgents,
    "shadow-canary": {
      ...candidateAgent,
      mode: "primary",
      steps: 1,
      permission: { ...recordValue(candidateAgent.permission), "*": "deny" },
    },
  },
}

const home = join(process.cwd(), ".candidate-home")
await mkdir(home, { recursive: true, mode: 0o700 })
const cli = join(checkout, "packages/opencode/src/index.ts")
const child = spawn(
  process.execPath,
  [
    "run",
    "--conditions=browser",
    cli,
    "--pure",
    "run",
    "--format",
    "json",
    "--model",
    `${providerID}/${modelID}`,
    "--agent",
    "shadow-canary",
    "--title",
    "shadow-canary",
    "--dir",
    process.cwd(),
    "--",
    prompt,
  ],
  {
    cwd: checkout,
    env: {
      ...process.env,
      HOME: home,
      XDG_CONFIG_HOME: join(home, ".config"),
      XDG_DATA_HOME: join(home, ".local", "share"),
      XDG_STATE_HOME: join(home, ".local", "state"),
      XDG_CACHE_HOME: join(home, ".cache"),
      OPENCODE_TEST_HOME: home,
      OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
      OPENCODE_AUTH_CONTENT: process.env.OPENCODE_SHADOW_CANDIDATE_AUTH_CONTENT ?? "{}",
      OPENCODE_DISABLE_PROJECT_CONFIG: "1",
      OPENCODE_DISABLE_AUTOUPDATE: "1",
      OPENCODE_DISABLE_AUTOCOMPACT: "1",
      OPENCODE_DISABLE_MODELS_FETCH: "1",
      OPENCODE_PURE: "1",
      OPENCODE_SHADOW_CANARY_ENABLED: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  },
)
const stdout: Buffer[] = []
child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk))
child.stderr.resume()
const exitCode = await new Promise<number | null>((done, fail) => {
  child.once("error", fail)
  child.once("close", done)
})

const events = Buffer.concat(stdout)
  .toString("utf8")
  .split(/\r?\n/)
  .filter(Boolean)
  .flatMap((line) => {
    try {
      return [JSON.parse(line) as Record<string, unknown>]
    } catch {
      return []
    }
  })
const errorEvent = events.find((event) => event.type === "error")
const output = events
  .filter((event) => event.type === "text" || event.type === "reasoning")
  .map((event) => JSON.stringify(recordValue(event.part)))
const tools = events.filter((event) => event.type === "tool_use").map((event) => JSON.stringify(recordValue(event.part)))
const finishes = events.filter((event) => event.type === "step_finish").map((event) => recordValue(event.part))
const usage = finishes.reduce<{ input: number; output: number; reasoning: number }>(
  (sum, part) => {
    const tokens = recordValue(part.tokens)
    sum.input += numeric(tokens.input)
    sum.output += numeric(tokens.output)
    sum.reasoning += numeric(tokens.reasoning)
    return sum
  },
  { input: 0, output: 0, reasoning: 0 },
)
const cost = finishes.reduce((sum, part) => sum + numeric(part.cost), 0)
const failed = exitCode !== 0 || errorEvent !== undefined
const outcome: ShadowCanaryOutcome = {
  version: "opencode-shadow-canary-outcome.v1",
  source: "candidate",
  requestDigest,
  invocationID,
  status: failed ? "failed" : "completed",
  durationMs: Date.now() - started,
  outputDigest: sha256(output.join("\n")),
  toolPlanDigest: sha256(tools.join("\n")),
  eventCount: events.length,
  toolCalls: tools.length,
  usage: { ...usage, total: usage.input + usage.output + usage.reasoning },
  cost: { total: cost, currency: "USD" },
  ...(failed
    ? {
        error:
          errorEvent !== undefined
            ? "candidate OpenCode emitted an error event"
            : `candidate OpenCode exited with code ${String(exitCode)}`,
      }
    : {}),
}
process.stdout.write(`${JSON.stringify(outcome)}\n`)

function required(name: string) {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is required`)
  return value
}

function recordValue(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {}
}

function parseObject(value: string) {
  return recordValue(JSON.parse(value))
}

function text(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : undefined
}

function numeric(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0
}

function latestUserPrompt(request: Record<string, unknown>) {
  const messages = Array.isArray(request.messages) ? request.messages : []
  for (const item of [...messages].reverse()) {
    const message = recordValue(item)
    if (message.role !== "user") continue
    const result = collectText(message.content).join("\n").trim()
    if (result) return result
  }
  return undefined
}

function collectText(value: unknown): string[] {
  if (typeof value === "string") return [value]
  if (Array.isArray(value)) return value.flatMap(collectText)
  const item = recordValue(value)
  if (typeof item.text === "string") return [item.text]
  return []
}

function sha256(value: string) {
  return createHash("sha256").update(value).digest("hex")
}
