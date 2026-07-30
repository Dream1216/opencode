import { mkdtemp, readFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { spawn } from "node:child_process"

type JsonRecord = Record<string, unknown>

const packageDir = path.resolve(import.meta.dir, "..")
const storeDir = await mkdtemp(path.join(tmpdir(), "opencode-shadow-agent-run-"))
const verifyDirectory = process.env.OPENCODE_SHADOW_VERIFY_DIRECTORY || packageDir

const server = spawn(
  process.execPath,
  ["run", "--conditions=browser", "./src/index.ts", "serve", "--hostname", "127.0.0.1", "--port", "0"],
  {
    cwd: packageDir,
    env: {
      ...process.env,
      OPENCODE_SHADOW_EVENT_STORE_DIR: storeDir,
    },
    stdio: ["ignore", "pipe", "pipe"],
  },
)

try {
  const url = await waitForServer(server)
  const response = await fetch(new URL("/session", url), {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-opencode-directory": verifyDirectory,
    },
    body: "{}",
  })
  if (!response.ok) {
    throw new Error(`session create failed: HTTP ${response.status} ${await response.text()}`)
  }

  const session = (await response.json()) as JsonRecord
  await waitForEventFile()
  const runs = await readJsonLines(path.join(storeDir, "agent_runs.jsonl"))
  const events = await readJsonLines(path.join(storeDir, "agent_run_events.jsonl"))
  const runStarted = events.find((event) => event.type === "run.started" && event.sessionID === session.id)
  if (!runStarted) throw new Error(`missing run.started for session ${String(session.id)}`)
  const run = runs.find((item) => item.sessionID === session.id)
  if (!run) throw new Error(`missing run snapshot for session ${String(session.id)}`)

  console.log(
    JSON.stringify(
      {
        ok: true,
        sessionID: session.id,
        storeDir,
        runs: runs.length,
        events: events.length,
      },
      null,
      2,
    ),
  )
} finally {
  server.kill("SIGTERM")
}

function waitForServer(child: typeof server) {
  return new Promise<URL>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for opencode server")), 30_000)
    const onData = (chunk: Buffer) => {
      const text = chunk.toString("utf8")
      const match = text.match(/opencode server listening on (http:\/\/[^\s]+)/)
      if (!match) return
      clearTimeout(timer)
      resolve(new URL(match[1]))
    }
    child.stdout.on("data", onData)
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString("utf8")
      if (text.toLowerCase().includes("error")) {
        clearTimeout(timer)
        reject(new Error(text.trim()))
      }
    })
    child.on("exit", (code) => {
      clearTimeout(timer)
      reject(new Error(`opencode server exited before listening: ${code}`))
    })
  })
}

async function waitForEventFile() {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    const events = await readJsonLines(path.join(storeDir, "agent_run_events.jsonl")).catch(() => [])
    if (events.length > 0) return
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for shadow event file in ${storeDir}`)
}

async function readJsonLines(file: string): Promise<JsonRecord[]> {
  const text = await readFile(file, "utf8")
  return text
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as JsonRecord)
}
