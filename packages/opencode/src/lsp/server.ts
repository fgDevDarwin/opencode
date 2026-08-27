import type { ChildProcessWithoutNullStreams } from "child_process"
import { PassThrough } from "stream"
import net from "net"
import crypto from "crypto"
import path from "path"
import os from "os"
import { fileURLToPath, pathToFileURL } from "url"
import { Global } from "@opencode-ai/core/global"
import { text } from "node:stream/consumers"
import fs from "fs/promises"
import fsSync from "fs"
import { Filesystem } from "@/util/filesystem"
import type { InstanceContext } from "../project/instance-context"
import { Archive } from "@/util/archive"
import { Process } from "@/util/process"
import { which } from "@opencode-ai/core/util/which"
import { Module } from "@opencode-ai/core/util/module"
import { spawn } from "./launch"
import { Npm } from "@opencode-ai/core/npm"
import type { RuntimeFlags } from "@/effect/runtime-flags"

const pathExists = async (p: string) =>
  fs
    .stat(p)
    .then(() => true)
    .catch(() => false)
const run = (cmd: string[], opts: Process.RunOptions = {}) => Process.run(cmd, { ...opts, nothrow: true })
const output = (cmd: string[], opts: Process.RunOptions = {}) => Process.text(cmd, { ...opts, nothrow: true })

export interface Handle {
  process: ChildProcessWithoutNullStreams
  initialization?: Record<string, any>
}

export type TerraformPoolStatus = {
  mode: "pooled" | "direct"
  rootFingerprint: string
  workerPID?: number
  active: boolean
}

const terraformProcessStatus = new WeakMap<object, TerraformPoolStatus>()

export function terraformPoolStatus(process: object) {
  return terraformProcessStatus.get(process)
}

function deactivateTerraformProcess(process: object) {
  const status = terraformProcessStatus.get(process)
  if (status) status.active = false
}

type RootFunction = (file: string, ctx: InstanceContext) => Promise<string | undefined>

const NearestRoot = (includePatterns: string[], excludePatterns?: string[]): RootFunction => {
  return async (file, ctx) => {
    if (excludePatterns) {
      const excludedFiles = Filesystem.up({
        targets: excludePatterns,
        start: path.dirname(file),
        stop: ctx.directory,
      })
      const excluded = await excludedFiles.next()
      await excludedFiles.return()
      if (excluded.value) return undefined
    }
    const files = Filesystem.up({
      targets: includePatterns,
      start: path.dirname(file),
      stop: ctx.directory,
    })
    const first = await files.next()
    await files.return()
    if (!first.value) return ctx.directory
    return path.dirname(first.value)
  }
}

const StrictNearestRoot = (includePatterns: string[], excludePatterns?: string[]): RootFunction => {
  return async (file, ctx) => {
    if (excludePatterns) {
      const excludedFiles = Filesystem.up({
        targets: excludePatterns,
        start: path.dirname(file),
        stop: ctx.directory,
      })
      const excluded = await excludedFiles.next()
      await excludedFiles.return()
      if (excluded.value) return undefined
    }
    const files = Filesystem.up({
      targets: includePatterns,
      start: path.dirname(file),
      stop: ctx.directory,
    })
    const first = await files.next()
    await files.return()
    if (!first.value) return undefined
    return path.dirname(first.value)
  }
}

export interface Info {
  id: string
  extensions: string[]
  global?: boolean
  root: RootFunction
  spawn(root: string, ctx: InstanceContext, flags: RuntimeFlags.Info): Promise<Handle | undefined>
}

// Terraform's built-in server is deliberately the only pooled server.  The
// broker is process-local for now; this keeps the public LSP transport
// unchanged while ensuring that each client still gets a private stream.
type TerraformBroker = {
  process: ChildProcessWithoutNullStreams
  roots: Map<string, number>
  initialized?: { capabilities?: Record<string, unknown> }
  clients: Set<{ root: string; input: PassThrough; output: PassThrough; closed: boolean }>
  nextID: number
  pending: Map<number, { client: { root: string; input: PassThrough; output: PassThrough; closed: boolean }; id?: number | string }>
  serverRequests: Map<string, number>
  documentOwners: Map<string, { root: string; input: PassThrough; output: PassThrough; closed: boolean }>
  clientRequests: Map<string, number>
  generation: number
}

const terraformBrokers = new Map<string, TerraformBroker>()
type TerraformStartup = {
  broker: TerraformBroker
  connection: Awaited<ReturnType<typeof connectTerraformBroker>>
  child: ChildProcessWithoutNullStreams
}

const terraformStarting = new Map<string, Promise<TerraformStartup | undefined>>()
const terraformIPC = new Map<string, { server: net.Server; secret: string; broker: TerraformBroker }>()
function terraformEndpointForKey(key: string) {
  return path.join(Global.Path.state, `terraform-lsp-broker-${crypto.createHash("sha256").update(key).digest("hex")}.json`)
}

function terraformSocketForEndpoint(endpoint: string) {
  const hash = path.basename(endpoint).match(/^terraform-lsp-broker-([0-9a-f]+)\.json$/)?.[1] ?? "broker"
  return path.join(path.dirname(endpoint), `terraform-lsp-${hash.slice(0, 16)}.sock`)
}
function lockPathForKey(key: string) {
  return path.join(Global.Path.state, `terraform-lsp-${crypto.createHash("sha256").update(key).digest("hex")}.lock`)
}

function cleanupTerraformBroker(key: string, broker: TerraformBroker, server?: net.Server, endpoint = terraformEndpointForKey(key)) {
  const ipc = terraformIPC.get(key)
  ipc?.server.close()
  server?.close()
  terraformIPC.delete(key)
  terraformBrokers.delete(key)
  for (const artifact of [endpoint, terraformSocketForEndpoint(endpoint), lockPathForKey(key)]) {
    try {
      fsSync.rmSync(artifact, { force: true })
    } catch {}
  }
}

function frame(message: unknown) {
  const body = JSON.stringify(message)
  return `Content-Length: ${Buffer.byteLength(body, "utf8")}\r\n\r\n${body}`
}

type TerraformMessage = {
  id?: number
  method?: string
  params?: { uri?: string; diagnostics?: unknown[] }
  result?: unknown
}

function canonicalRoot(root: string) {
  return fs.realpath(root).catch(() => path.resolve(root))
}

function canonicalPathSync(file: string) {
  try {
    return fsSync.realpathSync(file)
  } catch {
    return path.resolve(file)
  }
}

function diagnosticsForRoot(message: { params?: { diagnostics?: unknown[] } }, root: string) {
  const diagnostics = message.params?.diagnostics
  if (!Array.isArray(diagnostics)) return message
  return {
    ...message,
    params: {
      ...message.params,
      diagnostics: diagnostics.map((diagnostic) => {
        if (!diagnostic || typeof diagnostic !== "object") return diagnostic
        const relatedInformation = (diagnostic as { relatedInformation?: unknown[] }).relatedInformation
        if (!Array.isArray(relatedInformation)) return diagnostic
        return {
          ...diagnostic,
          relatedInformation: relatedInformation.filter((related) => {
            if (!related || typeof related !== "object") return false
            const uri = (related as { location?: { uri?: unknown } }).location?.uri
            if (typeof uri !== "string" || !uri.startsWith("file:")) return true
            try {
              return insideRoot(canonicalPathSync(fileURLToPath(uri)), root)
            } catch {
              return false
            }
          }),
        }
      }),
    },
  }
}

function resultForRoot(message: { result?: unknown }, root: string) {
  if (!Array.isArray(message.result)) return message
  return {
    ...message,
    result: message.result.filter((item) => {
      if (!item || typeof item !== "object") return true
      const uri = (item as { location?: { uri?: unknown } }).location?.uri
      if (typeof uri !== "string" || !uri.startsWith("file:")) return true
      try {
        return insideRoot(canonicalPathSync(fileURLToPath(uri)), root)
      } catch {
        return false
      }
    }),
  }
}

function insideRoot(file: string, root: string) {
  const relative = path.relative(root, file)
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
}

function attachTerraformClient(broker: TerraformBroker, root: string) {
  const input = new PassThrough()
  const output = new PassThrough()
  const client = { root, input, output, closed: false }
  broker.clients.add(client)
  let buffer = Buffer.alloc(0)
  const send = (message: unknown) => output.write(frame(message))
  const roots = () => [...broker.roots.keys()].map((uri) => ({ name: "workspace", uri }))
  const handle = async (raw: string) => {
    const message = JSON.parse(raw) as { id?: number | string; method?: string; params?: Record<string, unknown> }
    if (!message.method && message.id !== undefined) {
      const upstreamID = broker.serverRequests.get(`${root}:${message.id}`)
      if (upstreamID !== undefined) {
        broker.serverRequests.delete(`${root}:${message.id}`)
        broker.process.stdin.write(frame({ ...message, id: upstreamID }))
        return
      }
    }
    if (message.method === "$ /cancelRequest" || message.method === "$/cancelRequest") {
      const requestID = (message.params as { id?: number | string } | undefined)?.id
      const upstreamID = requestID === undefined ? undefined : broker.clientRequests.get(`${root}:${requestID}`)
      if (upstreamID !== undefined) broker.process.stdin.write(frame({ ...message, params: { ...(message.params ?? {}), id: upstreamID } }))
      return
    }
    const candidate = (message.params?.uri ?? (message.params?.textDocument as Record<string, unknown> | undefined)?.uri) as string | undefined
    if (candidate?.startsWith("file:")) {
      const target = fileURLToPath(candidate)
      const canonicalFile = await fs.realpath(target).catch(() => path.resolve(target))
      broker.documentOwners.set(canonicalFile, client)
    }
    if (candidate?.startsWith("file:")) {
      const file = fileURLToPath(candidate)
      const canonicalFile = await fs.realpath(file).catch(() => path.resolve(file))
      if (!insideRoot(canonicalFile, root)) {
        if (message.id !== undefined) send({ jsonrpc: "2.0", id: message.id, error: { code: -32602, message: "URI is outside the workspace root" } })
        return
      }
    }
    if (message.method === "initialize") {
      if (broker.initialized) {
        send({ jsonrpc: "2.0", id: message.id, result: broker.initialized })
        return
      }
      const params = { ...(message.params ?? {}), rootUri: null, workspaceFolders: roots(), capabilities: { ...(message.params?.capabilities as Record<string, unknown>), workspace: { ...((message.params?.capabilities as any)?.workspace ?? {}), workspaceFolders: true } } }
      const upstreamID = broker.nextID++
      broker.pending.set(upstreamID, { client, id: message.id })
      broker.process.stdin.write(frame({ jsonrpc: "2.0", id: upstreamID, method: "initialize", params }))
      return
    }
    if (message.method === "test/get-initialize-params") {
      send({ jsonrpc: "2.0", id: message.id, result: { rootUri: null, workspaceFolders: roots(), capabilities: { workspace: { workspaceFolders: true } } } })
      return
    }
    if (message.method === "workspace/workspaceFolders") {
      send({ jsonrpc: "2.0", id: message.id, result: [{ name: "workspace", uri: pathToFileURL(root).href }] })
      return
    }
    const id = message.id === undefined ? undefined : broker.nextID++
    if (id !== undefined) {
      broker.pending.set(id, { client, id: message.id })
      broker.clientRequests.set(`${root}:${message.id}`, id)
    }
    broker.process.stdin.write(frame({ ...message, ...(id === undefined ? {} : { id }) }))
  }
  input.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk])
    while (true) {
      const end = buffer.indexOf("\r\n\r\n")
      if (end < 0) break
      const match = /Content-Length:\s*(\d+)/i.exec(buffer.subarray(0, end).toString())
      if (!match) { buffer = buffer.subarray(end + 4); continue }
      const length = Number(match[1])
      if (buffer.length < end + 4 + length) break
      void handle(buffer.subarray(end + 4, end + 4 + length).toString())
      buffer = buffer.subarray(end + 4 + length)
    }
  })
  return { client, input, output }
}

function startTerraformBroker(child: ChildProcessWithoutNullStreams, key: string, root: string, endpoint = terraformEndpointForKey(key)) {
  const broker: TerraformBroker = {
    process: child,
    roots: new Map(),
    clients: new Set(),
    nextID: 1,
    pending: new Map(),
    serverRequests: new Map(),
    documentOwners: new Map(),
    clientRequests: new Map(),
    generation: 1,
  }
  let buffer = Buffer.alloc(0)
  child.stdout.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk])
    while (true) {
      const end = buffer.indexOf("\r\n\r\n")
      if (end < 0) break
      const match = /Content-Length:\s*(\d+)/i.exec(buffer.subarray(0, end).toString())
      if (!match) { buffer = buffer.subarray(end + 4); continue }
      const length = Number(match[1])
      if (buffer.length < end + 4 + length) break
       const message = JSON.parse(buffer.subarray(end + 4, end + 4 + length).toString()) as TerraformMessage
      buffer = buffer.subarray(end + 4 + length)
      if (message.method && message.id !== undefined) {
        const candidate = message.params?.uri
        const target = candidate?.startsWith("file:")
          ? [...broker.clients].find((client) => !client.closed && insideRoot(fileURLToPath(candidate), client.root))
          : [...broker.documentOwners.values()].find((client) => !client.closed) ?? [...broker.clients].find((client) => !client.closed)
        if (target && !target.closed) {
          broker.serverRequests.set(`${target.root}:${message.id}`, message.id)
          target.output.write(frame(message))
        }
        continue
      }
      if (message.id !== undefined) {
        const pending = broker.pending.get(message.id)
        if (pending) {
          broker.pending.delete(message.id)
          if (pending.id !== undefined) broker.clientRequests.delete(`${pending.client.root}:${pending.id}`)
          if (!broker.initialized) {
            const capabilities = (message as { result?: { capabilities?: Record<string, unknown> } }).result?.capabilities
            const workspace = capabilities?.workspace as Record<string, unknown> | undefined
            const folders = workspace?.workspaceFolders as Record<string, unknown> | undefined
            if (folders?.supported !== true || folders.changeNotifications !== true) {
              pending.client.output.write(frame({ jsonrpc: "2.0", id: pending.id, error: { code: -32001, message: "Terraform server lacks dynamic workspace-folder support" } }))
              return
            }
            broker.initialized = { capabilities }
          }
          if (!pending.client.closed) pending.client.output.write(frame({ ...resultForRoot(message, pending.client.root), id: pending.id }))
        }
        continue
      }
      if (message.method === "textDocument/publishDiagnostics" && message.params?.uri?.startsWith("file:")) {
        const file = fileURLToPath(message.params.uri)
        const canonicalFile = canonicalPathSync(file)
        for (const client of broker.clients) {
          if (!client.closed && insideRoot(canonicalFile, client.root)) client.output.write(frame(diagnosticsForRoot(message, client.root)))
        }
        continue
      }
      const uri = (message.params as { uri?: string } | undefined)?.uri
      if (!uri?.startsWith("file:")) continue
      const file = fileURLToPath(uri)
      const canonicalFile = canonicalPathSync(file)
      for (const client of broker.clients) if (!client.closed && insideRoot(canonicalFile, client.root)) client.output.write(frame(message))
    }
  })
  child.once("exit", () => {
    broker.generation += 1
    for (const pending of broker.pending.values()) {
      if (!pending.client.closed) pending.client.output.write(frame({ jsonrpc: "2.0", id: pending.id, error: { code: -32002, message: "Terraform worker exited" } }))
    }
    broker.pending.clear()
    broker.clientRequests.clear()
    // A dead generation must never remain usable. Closing every virtual stream
    // makes the owning LSP client observe the failure and recreate/fall back
    // instead of sending traffic into a lost worker.
    for (const client of broker.clients) {
      client.closed = true
      client.input.end()
      client.output.end()
    }
    broker.clients.clear()
    broker.roots.clear()
    cleanupTerraformBroker(key, broker, undefined, endpoint)
  })
  terraformBrokers.set(key, broker)
  return broker
}

async function connectTerraformBroker(key: string, root: string, child: ChildProcessWithoutNullStreams) {
  const endpoint = terraformEndpointForKey(key)
  const socketPath = terraformSocketForEndpoint(endpoint)
  const existing = terraformIPC.get(key)
  if (existing) return open(socketPath, existing.secret, root, existing.broker)
  const secret = crypto.randomBytes(32).toString("hex")
  const broker = startTerraformBroker(child, key, root, endpoint)
  const server = net.createServer((socket) => {
    let authenticated = false
    let pending = Buffer.alloc(0)
    const authenticate = async () => {
      const end = pending.indexOf("\r\n\r\n")
      if (end < 0) return
      const match = /Content-Length:\s*(\d+)/i.exec(pending.subarray(0, end).toString())
      if (!match) return socket.destroy()
      const length = Number(match[1])
      if (pending.length < end + 4 + length) return
      let auth: { secret?: string; root?: string }
      try { auth = JSON.parse(pending.subarray(end + 4, end + 4 + length).toString()) as { secret?: string; root?: string } } catch { return socket.destroy() }
      const expected = Buffer.from(secret)
      const actual = Buffer.from(auth.secret ?? "")
      if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected) || !auth.root) return socket.destroy()
      authenticated = true
      const canonical = await fs.realpath(auth.root).catch(() => path.resolve(auth.root!))
      const attached = attachTerraformClient(broker, canonical)
      const uri = pathToFileURL(canonical).href
      const previous = broker.roots.get(uri) ?? 0
      broker.roots.set(uri, previous + 1)
      if (previous === 0 && broker.initialized) {
        broker.process.stdin.write(frame({ jsonrpc: "2.0", method: "workspace/didChangeWorkspaceFolders", params: { event: { added: [{ name: "workspace", uri }], removed: [] } } }))
      }
      socket.write(frame({ jsonrpc: "2.0", method: "terraform/authenticated" }))
      socket.removeListener("data", authenticate)
      socket.pipe(attached.input)
      attached.output.pipe(socket)
      const leftover = pending.subarray(end + 4 + length)
      pending = Buffer.alloc(0)
      if (leftover.length) attached.input.write(leftover)
      socket.on("close", () => {
        if (attached.client.closed) return
        attached.client.closed = true
        broker.clients.delete(attached.client)
        const leases = (broker.roots.get(uri) ?? 1) - 1
        if (leases <= 0) {
          broker.roots.delete(uri)
          if (broker.initialized) broker.process.stdin.write(frame({ jsonrpc: "2.0", method: "workspace/didChangeWorkspaceFolders", params: { event: { added: [], removed: [{ name: "workspace", uri }] } } }))
        } else broker.roots.set(uri, leases)
        if (!broker.clients.size) {
          void Process.stop(broker.process)
          cleanupTerraformBroker(key, broker, server, endpoint)
        }
      })
    }
    socket.on("data", (chunk) => {
      if (authenticated) return
      pending = Buffer.concat([pending, chunk])
      void authenticate()
    })
  })
  await fs.rm(socketPath, { force: true })
  await new Promise<void>((resolve, reject) => server.listen(socketPath, () => resolve()).once("error", reject))
  try {
    await fs.chmod(socketPath, 0o600)
    const mode = (await fs.stat(socketPath)).mode
    if ((mode & 0o077) !== 0) throw new Error("Terraform broker socket is not private")
  } catch (error) {
    server.close()
    await Process.stop(child)
    throw error
  }
  terraformIPC.set(key, { server, secret, broker })
  await fs.mkdir(Global.Path.state, { recursive: true })
  const registryTemp = `${endpoint}.${process.pid}.${crypto.randomBytes(6).toString("hex")}`
  await fs.writeFile(registryTemp, JSON.stringify({ key, socket: socketPath, secret, pid: globalThis.process.pid, workerPid: child.pid }), { mode: 0o600 })
  await fs.chmod(registryTemp, 0o600)
  await fs.rename(registryTemp, endpoint)
  return await open(socketPath, secret, root, broker)
}

async function open(address: string, secret: string, root: string, broker?: TerraformBroker) {
  const socket = net.createConnection(address)
  await new Promise<void>((resolve, reject) => {
    let buffer = Buffer.alloc(0)
    const onData = (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk])
      const end = buffer.indexOf("\r\n\r\n")
      if (end < 0) return
      const match = /Content-Length:\s*(\d+)/i.exec(buffer.subarray(0, end).toString())
      if (!match) return reject(new Error("Invalid Terraform broker authentication response"))
      const length = Number(match[1])
      if (buffer.length < end + 4 + length) return
      try {
        const response = JSON.parse(buffer.subarray(end + 4, end + 4 + length).toString()) as { method?: string }
        if (response.method !== "terraform/authenticated") return reject(new Error("Invalid Terraform broker authentication response"))
      } catch (error) {
        return reject(error)
      }
      socket.removeListener("data", onData)
      const leftover = buffer.subarray(end + 4 + length)
      if (leftover.length) socket.unshift(leftover)
      resolve()
    }
    socket.once("error", reject)
    socket.on("data", onData)
    socket.write(frame({ auth: true, secret, root }))
  })
  return { socket, broker }
}

function terraformDirect(bin: string, root: string, initialization: Record<string, unknown>, rootFingerprint: string) {
  const process = spawn(bin, ["serve"], { cwd: root })
  terraformProcessStatus.set(process, { mode: "direct", rootFingerprint, active: true })
  process.once("exit", () => deactivateTerraformProcess(process))
  return { process, initialization }
}

async function terraformSupportsMultiRoot(child: ChildProcessWithoutNullStreams, root: string, initialization: Record<string, unknown>) {
  const supported = new Promise<boolean>((resolve) => {
    let buffer = Buffer.alloc(0)
    const onData = (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk])
      const end = buffer.indexOf("\r\n\r\n")
      if (end < 0) return
      const match = /Content-Length:\s*(\d+)/i.exec(buffer.subarray(0, end).toString())
      if (!match) return resolve(false)
      const length = Number(match[1])
      if (buffer.length < end + 4 + length) return
      child.stdout.removeListener("data", onData)
      try {
        const response = JSON.parse(buffer.subarray(end + 4, end + 4 + length).toString()) as { result?: { capabilities?: { workspace?: { workspaceFolders?: { supported?: boolean; changeNotifications?: boolean } } } } }
        const folders = response.result?.capabilities?.workspace?.workspaceFolders
        resolve(folders?.supported === true && folders.changeNotifications === true)
      } catch {
        resolve(false)
      }
    }
    child.stdout.on("data", onData)
    child.stdin.write(frame({ jsonrpc: "2.0", id: 1, method: "initialize", params: { rootUri: null, workspaceFolders: [{ name: "workspace", uri: pathToFileURL(root).href }], initializationOptions: initialization, capabilities: { workspace: { workspaceFolders: true } } } }))
    setTimeout(() => { child.stdout.removeListener("data", onData); resolve(false) }, 5000)
  })
  return supported
}

export const Deno: Info = {
  id: "deno",
  root: async (file, ctx) => {
    const files = Filesystem.up({
      targets: ["deno.json", "deno.jsonc"],
      start: path.dirname(file),
      stop: ctx.directory,
    })
    const first = await files.next()
    await files.return()
    if (!first.value) return undefined
    return path.dirname(first.value)
  },
  extensions: [".ts", ".tsx", ".js", ".jsx", ".mjs"],
  async spawn(root) {
    const deno = which("deno")
    if (!deno) {
      return
    }
    return {
      process: spawn(deno, ["lsp"], {
        cwd: root,
      }),
    }
  },
}

export const Typescript: Info = {
  id: "typescript",
  root: NearestRoot(
    ["package-lock.json", "bun.lockb", "bun.lock", "pnpm-lock.yaml", "yarn.lock"],
    ["deno.json", "deno.jsonc"],
  ),
  extensions: [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts"],
  async spawn(root, ctx) {
    const tsserver = Module.resolve("typescript/lib/tsserver.js", ctx.directory)
    if (!tsserver) return
    const bin = await Npm.which("typescript-language-server")
    if (!bin) return
    const proc = spawn(bin, ["--stdio"], {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
      initialization: {
        tsserver: {
          path: tsserver,
        },
      },
    }
  },
}

export const Vue: Info = {
  id: "vue",
  extensions: [".vue"],
  root: NearestRoot(["package-lock.json", "bun.lockb", "bun.lock", "pnpm-lock.yaml", "yarn.lock"]),
  async spawn(root, _ctx, flags) {
    let binary = which("vue-language-server")
    const args: string[] = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("@vue/language-server")
      if (!resolved) return
      binary = resolved
    }
    args.push("--stdio")
    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
      initialization: {
        // Leave empty; the server will auto-detect workspace TypeScript.
      },
    }
  },
}

export const ESLint: Info = {
  id: "eslint",
  root: NearestRoot(["package-lock.json", "bun.lockb", "bun.lock", "pnpm-lock.yaml", "yarn.lock"]),
  extensions: [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts", ".vue"],
  async spawn(root, ctx, flags) {
    const eslint = Module.resolve("eslint", ctx.directory)
    if (!eslint) return
    const serverPath = path.join(Global.Path.bin, "vscode-eslint", "server", "out", "eslintServer.js")
    if (!(await Filesystem.exists(serverPath))) {
      if (flags.disableLspDownload) return
      const response = await fetch("https://github.com/microsoft/vscode-eslint/archive/refs/heads/main.zip")
      if (!response.ok) return

      const zipPath = path.join(Global.Path.bin, "vscode-eslint.zip")
      if (response.body) await Filesystem.writeStream(zipPath, response.body)

      const ok = await Archive.extractZip(zipPath, Global.Path.bin)
        .then(() => true)
        .catch((error) => {
          return false
        })
      if (!ok) return
      await fs.rm(zipPath, { force: true })

      const extractedPath = path.join(Global.Path.bin, "vscode-eslint-main")
      const finalPath = path.join(Global.Path.bin, "vscode-eslint")

      const stats = await fs.stat(finalPath).catch(() => undefined)
      if (stats) {
        await fs.rm(finalPath, { force: true, recursive: true })
      }
      await fs.rename(extractedPath, finalPath)

      const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm"
      await Process.run([npmCmd, "install"], { cwd: finalPath })
      await Process.run([npmCmd, "run", "compile"], { cwd: finalPath })
    }

    const proc = spawn("node", [serverPath, "--stdio"], {
      cwd: root,
      env: {
        ...process.env,
      },
    })

    return {
      process: proc,
    }
  },
}

export const Oxlint: Info = {
  id: "oxlint",
  root: NearestRoot([
    ".oxlintrc.json",
    "package-lock.json",
    "bun.lockb",
    "bun.lock",
    "pnpm-lock.yaml",
    "yarn.lock",
    "package.json",
  ]),
  extensions: [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts", ".vue", ".astro", ".svelte"],
  async spawn(root, ctx) {
    const ext = process.platform === "win32" ? ".cmd" : ""

    const serverTarget = path.join("node_modules", ".bin", "oxc_language_server" + ext)
    const lintTarget = path.join("node_modules", ".bin", "oxlint" + ext)

    const resolveBin = async (target: string) => {
      const localBin = path.join(root, target)
      if (await Filesystem.exists(localBin)) return localBin

      const candidates = Filesystem.up({
        targets: [target],
        start: root,
        stop: ctx.worktree,
      })
      const first = await candidates.next()
      await candidates.return()
      if (first.value) return first.value

      return undefined
    }

    let lintBin = await resolveBin(lintTarget)
    if (!lintBin) {
      const found = which("oxlint")
      if (found) lintBin = found
    }

    if (lintBin) {
      const proc = spawn(lintBin, ["--help"])
      await proc.exited
      if (proc.stdout) {
        const help = await text(proc.stdout)
        if (help.includes("--lsp")) {
          return {
            process: spawn(lintBin, ["--lsp"], {
              cwd: root,
            }),
          }
        }
      }
    }

    let serverBin = await resolveBin(serverTarget)
    if (!serverBin) {
      const found = which("oxc_language_server")
      if (found) serverBin = found
    }
    if (serverBin) {
      return {
        process: spawn(serverBin, [], {
          cwd: root,
        }),
      }
    }

    return
  },
}

export const Biome: Info = {
  id: "biome",
  root: NearestRoot([
    "biome.json",
    "biome.jsonc",
    "package-lock.json",
    "bun.lockb",
    "bun.lock",
    "pnpm-lock.yaml",
    "yarn.lock",
  ]),
  extensions: [
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".mjs",
    ".cjs",
    ".mts",
    ".cts",
    ".json",
    ".jsonc",
    ".vue",
    ".astro",
    ".svelte",
    ".css",
    ".graphql",
    ".gql",
    ".html",
  ],
  async spawn(root) {
    const localBin = path.join(root, "node_modules", ".bin", "biome")
    let bin: string | undefined
    if (await Filesystem.exists(localBin)) bin = localBin
    if (!bin) {
      const found = which("biome")
      if (found) bin = found
    }

    let args = ["lsp-proxy", "--stdio"]

    if (!bin) {
      const resolved = Module.resolve("biome", root)
      if (!resolved) return
      bin = await Npm.which("biome")
      if (!bin) return
      args = ["lsp-proxy", "--stdio"]
    }

    const proc = spawn(bin, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })

    return {
      process: proc,
    }
  },
}

export const Gopls: Info = {
  id: "gopls",
  root: async (file, ctx) => {
    const work = await NearestRoot(["go.work"])(file, ctx)
    if (work) return work
    return NearestRoot(["go.mod", "go.sum"])(file, ctx)
  },
  extensions: [".go"],
  async spawn(root, _ctx, flags) {
    let bin = which("gopls")
    if (!bin) {
      if (!which("go")) return
      if (flags.disableLspDownload) return

      const proc = Process.spawn(["go", "install", "golang.org/x/tools/gopls@latest"], {
        env: { ...process.env, GOBIN: Global.Path.bin },
        stdout: "pipe",
        stderr: "pipe",
        stdin: "pipe",
      })
      const exit = await proc.exited
      if (exit !== 0) {
        return
      }
      bin = path.join(Global.Path.bin, "gopls" + (process.platform === "win32" ? ".exe" : ""))
    }
    return {
      process: spawn(bin!, {
        cwd: root,
      }),
    }
  },
}

export const Rubocop: Info = {
  id: "ruby-lsp",
  root: NearestRoot(["Gemfile"]),
  extensions: [".rb", ".rake", ".gemspec", ".ru"],
  async spawn(root, _ctx, flags) {
    let bin = which("rubocop")
    if (!bin) {
      const ruby = which("ruby")
      const gem = which("gem")
      if (!ruby || !gem) {
        return
      }
      if (flags.disableLspDownload) return
      const proc = Process.spawn(["gem", "install", "rubocop", "--bindir", Global.Path.bin], {
        stdout: "pipe",
        stderr: "pipe",
        stdin: "pipe",
      })
      const exit = await proc.exited
      if (exit !== 0) {
        return
      }
      bin = path.join(Global.Path.bin, "rubocop" + (process.platform === "win32" ? ".exe" : ""))
    }
    return {
      process: spawn(bin!, ["--lsp"], {
        cwd: root,
      }),
    }
  },
}

export const Ty: Info = {
  id: "ty",
  extensions: [".py", ".pyi"],
  root: NearestRoot([
    "pyproject.toml",
    "ty.toml",
    "setup.py",
    "setup.cfg",
    "requirements.txt",
    "Pipfile",
    "pyrightconfig.json",
  ]),
  async spawn(root, _ctx, flags) {
    if (!flags.experimentalLspTy) {
      return undefined
    }

    let binary = which("ty")

    const initialization: Record<string, string> = {}

    const potentialVenvPaths = [process.env["VIRTUAL_ENV"], path.join(root, ".venv"), path.join(root, "venv")].filter(
      (p): p is string => p !== undefined,
    )
    for (const venvPath of potentialVenvPaths) {
      const isWindows = process.platform === "win32"
      const potentialPythonPath = isWindows
        ? path.join(venvPath, "Scripts", "python.exe")
        : path.join(venvPath, "bin", "python")
      if (await Filesystem.exists(potentialPythonPath)) {
        initialization["pythonPath"] = potentialPythonPath
        break
      }
    }

    if (!binary) {
      for (const venvPath of potentialVenvPaths) {
        const isWindows = process.platform === "win32"
        const potentialTyPath = isWindows ? path.join(venvPath, "Scripts", "ty.exe") : path.join(venvPath, "bin", "ty")
        if (await Filesystem.exists(potentialTyPath)) {
          binary = potentialTyPath
          break
        }
      }
    }

    if (!binary) {
      return
    }

    const proc = spawn(binary, ["server"], {
      cwd: root,
    })

    return {
      process: proc,
      initialization,
    }
  },
}

export const Pyright: Info = {
  id: "pyright",
  extensions: [".py", ".pyi"],
  root: NearestRoot(["pyproject.toml", "setup.py", "setup.cfg", "requirements.txt", "Pipfile", "pyrightconfig.json"]),
  async spawn(root, _ctx, flags) {
    let binary = which("pyright-langserver")
    const args = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("pyright", "pyright-langserver")
      if (!resolved) return
      binary = resolved
    }
    args.push("--stdio")

    const initialization: Record<string, string> = {}

    const potentialVenvPaths = [process.env["VIRTUAL_ENV"], path.join(root, ".venv"), path.join(root, "venv")].filter(
      (p): p is string => p !== undefined,
    )
    for (const venvPath of potentialVenvPaths) {
      const isWindows = process.platform === "win32"
      const potentialPythonPath = isWindows
        ? path.join(venvPath, "Scripts", "python.exe")
        : path.join(venvPath, "bin", "python")
      if (await Filesystem.exists(potentialPythonPath)) {
        initialization["pythonPath"] = potentialPythonPath
        break
      }
    }

    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
      initialization,
    }
  },
}

export const ElixirLS: Info = {
  id: "elixir-ls",
  extensions: [".ex", ".exs"],
  root: NearestRoot(["mix.exs", "mix.lock"]),
  async spawn(root, _ctx, flags) {
    let binary = which("elixir-ls")
    if (!binary) {
      const elixirLsPath = path.join(Global.Path.bin, "elixir-ls")
      binary = path.join(
        Global.Path.bin,
        "elixir-ls-master",
        "release",
        process.platform === "win32" ? "language_server.bat" : "language_server.sh",
      )

      if (!(await Filesystem.exists(binary))) {
        const elixir = which("elixir")
        if (!elixir) {
          return
        }

        if (flags.disableLspDownload) return

        const response = await fetch("https://github.com/elixir-lsp/elixir-ls/archive/refs/heads/master.zip")
        if (!response.ok) return
        const zipPath = path.join(Global.Path.bin, "elixir-ls.zip")
        if (response.body) await Filesystem.writeStream(zipPath, response.body)

        const ok = await Archive.extractZip(zipPath, Global.Path.bin)
          .then(() => true)
          .catch((error) => {
            return false
          })
        if (!ok) return

        await fs.rm(zipPath, {
          force: true,
          recursive: true,
        })

        const cwd = path.join(Global.Path.bin, "elixir-ls-master")
        const env = { MIX_ENV: "prod", ...process.env }
        await Process.run(["mix", "deps.get"], { cwd, env })
        await Process.run(["mix", "compile"], { cwd, env })
        await Process.run(["mix", "elixir_ls.release2", "-o", "release"], { cwd, env })
      }
    }

    return {
      process: spawn(binary, {
        cwd: root,
      }),
    }
  },
}

export const Zls: Info = {
  id: "zls",
  extensions: [".zig", ".zon"],
  root: NearestRoot(["build.zig"]),
  async spawn(root, _ctx, flags) {
    let bin = which("zls")

    if (!bin) {
      const zig = which("zig")
      if (!zig) {
        return
      }

      if (flags.disableLspDownload) return

      const releaseResponse = await fetch("https://api.github.com/repos/zigtools/zls/releases/latest")
      if (!releaseResponse.ok) {
        return
      }

      const release = (await releaseResponse.json()) as {
        assets?: { name?: string; browser_download_url?: string }[]
      }

      const platform = process.platform
      const arch = process.arch
      let assetName = ""

      let zlsArch: string = arch
      if (arch === "arm64") zlsArch = "aarch64"
      else if (arch === "x64") zlsArch = "x86_64"
      else if (arch === "ia32") zlsArch = "x86"

      let zlsPlatform: string = platform
      if (platform === "darwin") zlsPlatform = "macos"
      else if (platform === "win32") zlsPlatform = "windows"

      const ext = platform === "win32" ? "zip" : "tar.xz"

      assetName = `zls-${zlsArch}-${zlsPlatform}.${ext}`

      const supportedCombos = [
        "zls-x86_64-linux.tar.xz",
        "zls-x86_64-macos.tar.xz",
        "zls-x86_64-windows.zip",
        "zls-aarch64-linux.tar.xz",
        "zls-aarch64-macos.tar.xz",
        "zls-aarch64-windows.zip",
        "zls-x86-linux.tar.xz",
        "zls-x86-windows.zip",
      ]

      if (!supportedCombos.includes(assetName)) {
        return
      }

      const asset = release.assets?.find((a) => a.name === assetName)
      if (!asset?.browser_download_url) {
        return
      }

      const downloadUrl = asset.browser_download_url
      const downloadResponse = await fetch(downloadUrl)
      if (!downloadResponse.ok) {
        return
      }

      const tempPath = path.join(Global.Path.bin, assetName)
      if (downloadResponse.body) await Filesystem.writeStream(tempPath, downloadResponse.body)

      if (ext === "zip") {
        const ok = await Archive.extractZip(tempPath, Global.Path.bin)
          .then(() => true)
          .catch((error) => {
            return false
          })
        if (!ok) return
      } else {
        await run(["tar", "-xf", tempPath], { cwd: Global.Path.bin })
      }

      await fs.rm(tempPath, { force: true })

      bin = path.join(Global.Path.bin, "zls" + (platform === "win32" ? ".exe" : ""))

      if (!(await Filesystem.exists(bin))) {
        return
      }

      if (platform !== "win32") {
        await fs.chmod(bin, 0o755).catch(() => {})
      }
    }

    return {
      process: spawn(bin, {
        cwd: root,
      }),
    }
  },
}

export const CSharp: Info = {
  id: "csharp",
  root: NearestRoot([".slnx", ".sln", ".csproj", "global.json"]),
  extensions: [".cs", ".csx"],
  async spawn(root, _ctx, flags) {
    const bin = await getRoslynLanguageServer(flags.disableLspDownload)
    if (!bin) return

    return {
      process: spawn(bin, ["--stdio", "--autoLoadProjects"], {
        cwd: root,
      }),
    }
  },
}

export const Razor: Info = {
  id: "razor",
  root: NearestRoot([".slnx", ".sln", ".csproj", "global.json"]),
  extensions: [".razor", ".cshtml"],
  async spawn(root, _ctx, flags) {
    const bin = await getRoslynLanguageServer(flags.disableLspDownload)
    if (!bin) return

    const razor = await findVscodeRazorExtension()
    if (!razor) {
      return
    }

    return {
      process: spawn(
        bin,
        [
          "--stdio",
          "--autoLoadProjects",
          `--razorSourceGenerator=${razor.compiler}`,
          `--razorDesignTimePath=${razor.targets}`,
          "--extension",
          razor.extension,
        ],
        {
          cwd: root,
        },
      ),
    }
  },
}

let roslynLanguageServerInstall: Promise<string | undefined> | undefined

async function getRoslynLanguageServer(disableLspDownload: boolean) {
  const existing = which("roslyn-language-server")
  if (existing) return existing

  const global = await roslynLanguageServerGlobalPath()
  if (global) return global

  roslynLanguageServerInstall ||= installRoslynLanguageServer(disableLspDownload).finally(() => {
    roslynLanguageServerInstall = undefined
  })
  return roslynLanguageServerInstall
}

async function installRoslynLanguageServer(disableLspDownload: boolean) {
  if (!which("dotnet")) {
    return
  }

  if (disableLspDownload) return
  const proc = Process.spawn(["dotnet", "tool", "install", "--global", "roslyn-language-server", "--prerelease"], {
    stdout: "pipe",
    stderr: "pipe",
    stdin: "pipe",
  })
  const exit = await proc.exited
  if (exit !== 0) {
    return
  }

  const resolved = which("roslyn-language-server")
  if (resolved) {
    return resolved
  }

  const global = await roslynLanguageServerGlobalPath()
  if (global) {
    return global
  }
}

async function roslynLanguageServerGlobalPath() {
  const bin = path.join(
    process.env.DOTNET_CLI_HOME ?? os.homedir(),
    ".dotnet",
    "tools",
    "roslyn-language-server" + (process.platform === "win32" ? ".cmd" : ""),
  )
  return (await pathExists(bin)) ? bin : undefined
}

async function findVscodeRazorExtension() {
  const roots = [
    process.env.VSCODE_EXTENSIONS,
    path.join(os.homedir(), ".vscode", "extensions"),
    path.join(os.homedir(), ".vscode-insiders", "extensions"),
    path.join(os.homedir(), ".vscode-server", "extensions"),
    path.join(os.homedir(), ".vscode-server-insiders", "extensions"),
  ].filter((item) => item !== undefined)

  for (const root of [...new Set(roots)]) {
    const entries = await fs.readdir(root, { withFileTypes: true }).catch(() => [])
    const candidates = await Promise.all(
      entries
        .filter((entry) => entry.isDirectory() && entry.name.startsWith("ms-dotnettools.csharp-"))
        .map(async (entry) => ({
          path: path.join(root, entry.name, ".razorExtension"),
          modified: (await fs.stat(path.join(root, entry.name)).catch(() => undefined))?.mtimeMs ?? 0,
        })),
    )
    for (const entry of candidates.sort((a, b) => b.modified - a.modified).map((candidate) => candidate.path)) {
      const result = {
        compiler: path.join(entry, "Microsoft.CodeAnalysis.Razor.Compiler.dll"),
        targets: path.join(entry, "Targets", "Microsoft.NET.Sdk.Razor.DesignTime.targets"),
        extension: path.join(entry, "Microsoft.VisualStudioCode.RazorExtension.dll"),
      }
      if (
        (await pathExists(result.compiler)) &&
        (await pathExists(result.targets)) &&
        (await pathExists(result.extension))
      ) {
        return result
      }
    }
  }
}

export const FSharp: Info = {
  id: "fsharp",
  root: NearestRoot([".slnx", ".sln", ".fsproj", "global.json"]),
  extensions: [".fs", ".fsi", ".fsx", ".fsscript"],
  async spawn(root, _ctx, flags) {
    let bin = which("fsautocomplete")
    if (!bin) {
      if (!which("dotnet")) {
        return
      }

      if (flags.disableLspDownload) return
      const proc = Process.spawn(["dotnet", "tool", "install", "fsautocomplete", "--tool-path", Global.Path.bin], {
        stdout: "pipe",
        stderr: "pipe",
        stdin: "pipe",
      })
      const exit = await proc.exited
      if (exit !== 0) {
        return
      }

      bin = path.join(Global.Path.bin, "fsautocomplete" + (process.platform === "win32" ? ".exe" : ""))
    }

    return {
      process: spawn(bin, {
        cwd: root,
      }),
    }
  },
}

export const SourceKit: Info = {
  id: "sourcekit-lsp",
  extensions: [".swift", ".objc", "objcpp"],
  root: NearestRoot(["Package.swift", "*.xcodeproj", "*.xcworkspace"]),
  async spawn(root) {
    // Check if sourcekit-lsp is available in the PATH
    // This is installed with the Swift toolchain
    const sourcekit = which("sourcekit-lsp")
    if (sourcekit) {
      return {
        process: spawn(sourcekit, {
          cwd: root,
        }),
      }
    }

    // If sourcekit-lsp not found, check if xcrun is available
    // This is specific to macOS where sourcekit-lsp is typically installed with Xcode
    if (!which("xcrun")) return

    const lspLoc = await output(["xcrun", "--find", "sourcekit-lsp"])

    if (lspLoc.code !== 0) return

    const bin = lspLoc.text.trim()

    return {
      process: spawn(bin, {
        cwd: root,
      }),
    }
  },
}

export const RustAnalyzer: Info = {
  id: "rust",
  root: async (file, ctx) => {
    const crateRoot = await NearestRoot(["Cargo.toml", "Cargo.lock"])(file, ctx)
    if (crateRoot === undefined) {
      return undefined
    }
    let currentDir = crateRoot

    while (currentDir !== path.dirname(currentDir)) {
      // Stop at filesystem root
      const cargoTomlPath = path.join(currentDir, "Cargo.toml")
      try {
        const cargoTomlContent = await Filesystem.readText(cargoTomlPath)
        if (cargoTomlContent.includes("[workspace]")) {
          return currentDir
        }
      } catch {
        // File doesn't exist or can't be read, continue searching up
      }

      const parentDir = path.dirname(currentDir)
      if (parentDir === currentDir) break // Reached filesystem root
      currentDir = parentDir

      // Stop if we've gone above the app root
      if (!currentDir.startsWith(ctx.worktree)) break
    }

    return crateRoot
  },
  extensions: [".rs"],
  async spawn(root) {
    const bin = which("rust-analyzer")
    if (!bin) {
      return
    }
    return {
      process: spawn(bin, {
        cwd: root,
      }),
    }
  },
}

export const Clangd: Info = {
  id: "clangd",
  root: NearestRoot(["compile_commands.json", "compile_flags.txt", ".clangd"]),
  extensions: [".c", ".cpp", ".cc", ".cxx", ".c++", ".h", ".hpp", ".hh", ".hxx", ".h++"],
  async spawn(root, _ctx, flags) {
    const args = ["--background-index", "--clang-tidy"]
    const fromPath = which("clangd")
    if (fromPath) {
      return {
        process: spawn(fromPath, args, {
          cwd: root,
        }),
      }
    }

    const ext = process.platform === "win32" ? ".exe" : ""
    const direct = path.join(Global.Path.bin, "clangd" + ext)
    if (await Filesystem.exists(direct)) {
      return {
        process: spawn(direct, args, {
          cwd: root,
        }),
      }
    }

    const entries = await fs.readdir(Global.Path.bin, { withFileTypes: true }).catch(() => [])
    for (const entry of entries) {
      if (!entry.isDirectory()) continue
      if (!entry.name.startsWith("clangd_")) continue
      const candidate = path.join(Global.Path.bin, entry.name, "bin", "clangd" + ext)
      if (await Filesystem.exists(candidate)) {
        return {
          process: spawn(candidate, args, {
            cwd: root,
          }),
        }
      }
    }

    if (flags.disableLspDownload) return

    const releaseResponse = await fetch("https://api.github.com/repos/clangd/clangd/releases/latest")
    if (!releaseResponse.ok) {
      return
    }

    const release: {
      tag_name?: string
      assets?: { name?: string; browser_download_url?: string }[]
    } = await releaseResponse.json()

    const tag = release.tag_name
    if (!tag) {
      return
    }
    const platform = process.platform
    const tokens: Record<string, string> = {
      darwin: "mac",
      linux: "linux",
      win32: "windows",
    }
    const token = tokens[platform]
    if (!token) {
      return
    }

    const assets = release.assets ?? []
    const valid = (item: { name?: string; browser_download_url?: string }) => {
      if (!item.name) return false
      if (!item.browser_download_url) return false
      if (!item.name.includes(token)) return false
      return item.name.includes(tag)
    }

    const asset =
      assets.find((item) => valid(item) && item.name?.endsWith(".zip")) ??
      assets.find((item) => valid(item) && item.name?.endsWith(".tar.xz")) ??
      assets.find((item) => valid(item))
    if (!asset?.name || !asset.browser_download_url) {
      return
    }

    const name = asset.name
    const downloadResponse = await fetch(asset.browser_download_url)
    if (!downloadResponse.ok) {
      return
    }

    const archive = path.join(Global.Path.bin, name)
    const buf = await downloadResponse.arrayBuffer()
    if (buf.byteLength === 0) {
      return
    }
    await Filesystem.write(archive, Buffer.from(buf))

    const zip = name.endsWith(".zip")
    const tar = name.endsWith(".tar.xz")
    if (!zip && !tar) {
      return
    }

    if (zip) {
      const ok = await Archive.extractZip(archive, Global.Path.bin)
        .then(() => true)
        .catch((error) => {
          return false
        })
      if (!ok) return
    }
    if (tar) {
      await run(["tar", "-xf", archive], { cwd: Global.Path.bin })
    }
    await fs.rm(archive, { force: true })

    const bin = path.join(Global.Path.bin, "clangd_" + tag, "bin", "clangd" + ext)
    if (!(await Filesystem.exists(bin))) {
      return
    }

    if (platform !== "win32") {
      await fs.chmod(bin, 0o755).catch(() => {})
    }

    await fs.unlink(path.join(Global.Path.bin, "clangd")).catch(() => {})
    await fs.symlink(bin, path.join(Global.Path.bin, "clangd")).catch(() => {})

    return {
      process: spawn(bin, args, {
        cwd: root,
      }),
    }
  },
}

export const Svelte: Info = {
  id: "svelte",
  extensions: [".svelte"],
  root: NearestRoot(["package-lock.json", "bun.lockb", "bun.lock", "pnpm-lock.yaml", "yarn.lock"]),
  async spawn(root, _ctx, flags) {
    let binary = which("svelteserver")
    const args: string[] = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("svelte-language-server")
      if (!resolved) return
      binary = resolved
    }
    args.push("--stdio")
    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
      initialization: {},
    }
  },
}

export const Astro: Info = {
  id: "astro",
  extensions: [".astro"],
  root: NearestRoot(["package-lock.json", "bun.lockb", "bun.lock", "pnpm-lock.yaml", "yarn.lock"]),
  async spawn(root, ctx, flags) {
    const tsserver = Module.resolve("typescript/lib/tsserver.js", ctx.directory)
    if (!tsserver) {
      return
    }
    const tsdk = path.dirname(tsserver)

    let binary = which("astro-ls")
    const args: string[] = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("@astrojs/language-server")
      if (!resolved) return
      binary = resolved
    }
    args.push("--stdio")
    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
      initialization: {
        typescript: {
          tsdk,
        },
      },
    }
  },
}

function isModuleOf(pomContent: string, modulePath: string): boolean {
  const normalized = modulePath.replace(/\\/g, "/").replace(/\/$/, "")
  if (!normalized) return false
  const modulesBlocks = pomContent.match(/<modules>([\s\S]*?)<\/modules>/g) ?? []
  for (const block of modulesBlocks) {
    const stripped = block.replace(/<!--[\s\S]*?-->/g, "")
    for (const m of stripped.matchAll(/<module>\s*([^<]+?)\s*<\/module>/g)) {
      const decl = m[1].replace(/\\/g, "/").replace(/^\.\//, "").replace(/\/$/, "")
      if (decl === normalized) return true
    }
  }
  return false
}

export const JDTLS: Info = {
  id: "jdtls",
  root: async (file, ctx) => {
    const settingsMarkers = ["settings.gradle", "settings.gradle.kts"]
    const gradleMarkers = ["gradlew", "gradlew.bat"]
    // 1. Gradle (unchanged from original logic)
    const [wrapperRoot, settingsRoot] = await Promise.all([
      StrictNearestRoot(gradleMarkers, settingsMarkers)(file, ctx),
      StrictNearestRoot(settingsMarkers)(file, ctx),
    ])
    if (wrapperRoot) return wrapperRoot
    if (settingsRoot) return settingsRoot

    // 2. Gradle single-project fallback (build.gradle without settings.gradle)
    const buildRoot = await StrictNearestRoot(["build.gradle", "build.gradle.kts"])(file, ctx)
    if (buildRoot) return buildRoot

    // 3. Maven: walk up pom.xml chain verifying <module> relationships
    const pomFiles = await Filesystem.findUp("pom.xml", path.dirname(file), ctx.directory)
    if (pomFiles.length > 0) {
      let root = path.dirname(pomFiles[0])
      for (let i = 1; i < pomFiles.length; i++) {
        const parentDir = path.dirname(pomFiles[i])
        const rel = path.relative(parentDir, root)
        const content = await fs.readFile(pomFiles[i], "utf-8").catch(() => null)
        if (content && isModuleOf(content, rel)) {
          root = parentDir
        } else {
          break
        }
      }
      return root
    }

    // 4. Eclipse native project fallback
    const eclipseRoot = await StrictNearestRoot([".project", ".classpath"])(file, ctx)
    if (eclipseRoot) return eclipseRoot

    return undefined
  },
  extensions: [".java"],
  async spawn(root, _ctx, flags) {
    const java = which("java")
    if (!java) {
      return
    }
    const javaMajorVersion = await run(["java", "-version"]).then((result) => {
      const m = /"(\d+)\.\d+\.\d+"/.exec(result.stderr.toString())
      return !m ? undefined : parseInt(m[1])
    })
    if (javaMajorVersion == null || javaMajorVersion < 21) {
      return
    }
    const distPath = path.join(Global.Path.bin, "jdtls")
    const launcherDir = path.join(distPath, "plugins")
    const installed = await pathExists(launcherDir)
    if (!installed) {
      if (flags.disableLspDownload) return
      await fs.mkdir(distPath, { recursive: true })
      const releaseURL =
        "https://www.eclipse.org/downloads/download.php?file=/jdtls/snapshots/jdt-language-server-latest.tar.gz"
      const archiveName = "release.tar.gz"

      const download = await fetch(releaseURL)
      if (!download.ok || !download.body) {
        return
      }
      await Filesystem.writeStream(path.join(distPath, archiveName), download.body)

      const tarResult = await run(["tar", "-xzf", archiveName], { cwd: distPath })
      if (tarResult.code !== 0) {
        return
      }

      await fs.rm(path.join(distPath, archiveName), { force: true })
    }
    const jarFileName =
      (await fs.readdir(launcherDir).catch(() => []))
        .find((item) => /^org\.eclipse\.equinox\.launcher_.*\.jar$/.test(item))
        ?.trim() ?? ""
    const launcherJar = path.join(launcherDir, jarFileName)
    if (!(await pathExists(launcherJar))) {
      return
    }
    const configFile = path.join(
      distPath,
      (() => {
        switch (process.platform) {
          case "darwin":
            return "config_mac"
          case "linux":
            return "config_linux"
          case "win32":
            return "config_win"
          default:
            return "config_linux"
        }
      })(),
    )
    const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-jdtls-data"))
    return {
      process: spawn(
        java,
        [
          "-jar",
          launcherJar,
          "-configuration",
          configFile,
          "-data",
          dataDir,
          "-Declipse.application=org.eclipse.jdt.ls.core.id1",
          "-Dosgi.bundles.defaultStartLevel=4",
          "-Declipse.product=org.eclipse.jdt.ls.core.product",
          "-Dlog.level=ALL",
          "--add-modules=ALL-SYSTEM",
          "--add-opens java.base/java.util=ALL-UNNAMED",
          "--add-opens java.base/java.lang=ALL-UNNAMED",
        ],
        {
          cwd: root,
        },
      ),
    }
  },
}

export const KotlinLS: Info = {
  id: "kotlin-ls",
  extensions: [".kt", ".kts"],
  root: async (file, ctx) => {
    // 1) Nearest Gradle root (multi-project or included build)
    const settingsRoot = await NearestRoot(["settings.gradle.kts", "settings.gradle"])(file, ctx)
    if (settingsRoot) return settingsRoot
    // 2) Gradle wrapper (strong root signal)
    const wrapperRoot = await NearestRoot(["gradlew", "gradlew.bat"])(file, ctx)
    if (wrapperRoot) return wrapperRoot
    // 3) Single-project or module-level build
    const buildRoot = await NearestRoot(["build.gradle.kts", "build.gradle"])(file, ctx)
    if (buildRoot) return buildRoot
    // 4) Maven fallback
    return NearestRoot(["pom.xml"])(file, ctx)
  },
  async spawn(root, _ctx, flags) {
    const distPath = path.join(Global.Path.bin, "kotlin-ls")
    const launcherScript =
      process.platform === "win32" ? path.join(distPath, "kotlin-lsp.cmd") : path.join(distPath, "kotlin-lsp.sh")
    const installed = await Filesystem.exists(launcherScript)
    if (!installed) {
      if (flags.disableLspDownload) return

      const releaseResponse = await fetch("https://api.github.com/repos/Kotlin/kotlin-lsp/releases/latest")
      if (!releaseResponse.ok) {
        return
      }

      const release = await releaseResponse.json()
      const version = release.name?.replace(/^v/, "")

      if (!version) {
        return
      }

      const platform = process.platform
      const arch = process.arch

      let kotlinArch: string = arch
      if (arch === "arm64") kotlinArch = "aarch64"
      else if (arch === "x64") kotlinArch = "x64"

      let kotlinPlatform: string = platform
      if (platform === "darwin") kotlinPlatform = "mac"
      else if (platform === "linux") kotlinPlatform = "linux"
      else if (platform === "win32") kotlinPlatform = "win"

      const supportedCombos = ["mac-x64", "mac-aarch64", "linux-x64", "linux-aarch64", "win-x64", "win-aarch64"]

      const combo = `${kotlinPlatform}-${kotlinArch}`

      if (!supportedCombos.includes(combo)) {
        return
      }

      const assetName = `kotlin-lsp-${version}-${kotlinPlatform}-${kotlinArch}.zip`
      const releaseURL = `https://download-cdn.jetbrains.com/kotlin-lsp/${version}/${assetName}`

      await fs.mkdir(distPath, { recursive: true })
      const archivePath = path.join(distPath, "kotlin-ls.zip")
      const download = await fetch(releaseURL)
      if (!download.ok || !download.body) {
        return
      }
      await Filesystem.writeStream(archivePath, download.body)
      const ok = await Archive.extractZip(archivePath, distPath)
        .then(() => true)
        .catch((error) => {
          return false
        })
      if (!ok) return
      await fs.rm(archivePath, { force: true })
      if (process.platform !== "win32") {
        await fs.chmod(launcherScript, 0o755).catch(() => {})
      }
    }
    if (!(await Filesystem.exists(launcherScript))) {
      return
    }
    return {
      process: spawn(launcherScript, ["--stdio"], {
        cwd: root,
      }),
    }
  },
}

export const YamlLS: Info = {
  id: "yaml-ls",
  extensions: [".yaml", ".yml"],
  root: NearestRoot(["package-lock.json", "bun.lockb", "bun.lock", "pnpm-lock.yaml", "yarn.lock"]),
  async spawn(root, _ctx, flags) {
    let binary = which("yaml-language-server")
    const args: string[] = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("yaml-language-server")
      if (!resolved) return
      binary = resolved
    }
    args.push("--stdio")
    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
    }
  },
}

export const LuaLS: Info = {
  id: "lua-ls",
  root: NearestRoot([
    ".luarc.json",
    ".luarc.jsonc",
    ".luacheckrc",
    ".stylua.toml",
    "stylua.toml",
    "selene.toml",
    "selene.yml",
  ]),
  extensions: [".lua"],
  async spawn(root, _ctx, flags) {
    let bin = which("lua-language-server")

    if (!bin) {
      if (flags.disableLspDownload) return

      const releaseResponse = await fetch("https://api.github.com/repos/LuaLS/lua-language-server/releases/latest")
      if (!releaseResponse.ok) {
        return
      }

      const release = await releaseResponse.json()

      const platform = process.platform
      const arch = process.arch
      let assetName = ""

      let lualsArch: string = arch
      if (arch === "arm64") lualsArch = "arm64"
      else if (arch === "x64") lualsArch = "x64"
      else if (arch === "ia32") lualsArch = "ia32"

      let lualsPlatform: string = platform
      if (platform === "darwin") lualsPlatform = "darwin"
      else if (platform === "linux") lualsPlatform = "linux"
      else if (platform === "win32") lualsPlatform = "win32"

      const ext = platform === "win32" ? "zip" : "tar.gz"

      assetName = `lua-language-server-${release.tag_name}-${lualsPlatform}-${lualsArch}.${ext}`

      const supportedCombos = [
        "darwin-arm64.tar.gz",
        "darwin-x64.tar.gz",
        "linux-x64.tar.gz",
        "linux-arm64.tar.gz",
        "win32-x64.zip",
        "win32-ia32.zip",
      ]

      const assetSuffix = `${lualsPlatform}-${lualsArch}.${ext}`
      if (!supportedCombos.includes(assetSuffix)) {
        return
      }

      const asset = release.assets.find((a: any) => a.name === assetName)
      if (!asset) {
        return
      }

      const downloadUrl = asset.browser_download_url
      const downloadResponse = await fetch(downloadUrl)
      if (!downloadResponse.ok) {
        return
      }

      const tempPath = path.join(Global.Path.bin, assetName)
      if (downloadResponse.body) await Filesystem.writeStream(tempPath, downloadResponse.body)

      // Unlike zls which is a single self-contained binary,
      // lua-language-server needs supporting files (meta/, locale/, etc.)
      // Extract entire archive to dedicated directory to preserve all files
      const installDir = path.join(Global.Path.bin, `lua-language-server-${lualsArch}-${lualsPlatform}`)

      // Remove old installation if exists
      const stats = await fs.stat(installDir).catch(() => undefined)
      if (stats) {
        await fs.rm(installDir, { force: true, recursive: true })
      }

      await fs.mkdir(installDir, { recursive: true })

      if (ext === "zip") {
        const ok = await Archive.extractZip(tempPath, installDir)
          .then(() => true)
          .catch((error) => {
            return false
          })
        if (!ok) return
      } else {
        const ok = await run(["tar", "-xzf", tempPath, "-C", installDir])
          .then((result) => result.code === 0)
          .catch((error: unknown) => {
            return false
          })
        if (!ok) return
      }

      await fs.rm(tempPath, { force: true })

      // Binary is located in bin/ subdirectory within the extracted archive
      bin = path.join(installDir, "bin", "lua-language-server" + (platform === "win32" ? ".exe" : ""))

      if (!(await Filesystem.exists(bin))) {
        return
      }

      if (platform !== "win32") {
        const ok = await fs
          .chmod(bin, 0o755)
          .then(() => true)
          .catch((error: unknown) => {
            return false
          })
        if (!ok) return
      }
    }

    return {
      process: spawn(bin, {
        cwd: root,
      }),
    }
  },
}

export const PHPIntelephense: Info = {
  id: "php intelephense",
  extensions: [".php"],
  root: NearestRoot(["composer.json", "composer.lock", ".php-version"]),
  async spawn(root, _ctx, flags) {
    let binary = which("intelephense")
    const args: string[] = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("intelephense")
      if (!resolved) return
      binary = resolved
    }
    args.push("--stdio")
    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
      initialization: {
        telemetry: {
          enabled: false,
        },
      },
    }
  },
}

export const Prisma: Info = {
  id: "prisma",
  extensions: [".prisma"],
  root: NearestRoot(["schema.prisma", "prisma/schema.prisma", "prisma"], ["package.json"]),
  async spawn(root) {
    const prisma = which("prisma")
    if (!prisma) {
      return
    }
    return {
      process: spawn(prisma, ["language-server"], {
        cwd: root,
      }),
    }
  },
}

export const Dart: Info = {
  id: "dart",
  extensions: [".dart"],
  root: NearestRoot(["pubspec.yaml", "analysis_options.yaml"]),
  async spawn(root) {
    const dart = which("dart")
    if (!dart) {
      return
    }
    return {
      process: spawn(dart, ["language-server", "--lsp"], {
        cwd: root,
      }),
    }
  },
}

export const Ocaml: Info = {
  id: "ocaml-lsp",
  extensions: [".ml", ".mli"],
  root: NearestRoot(["dune-project", "dune-workspace", ".merlin", "opam"]),
  async spawn(root) {
    const bin = which("ocamllsp")
    if (!bin) {
      return
    }
    return {
      process: spawn(bin, {
        cwd: root,
      }),
    }
  },
}
export const BashLS: Info = {
  id: "bash",
  extensions: [".sh", ".bash", ".zsh", ".ksh"],
  root: async (_file, ctx) => ctx.directory,
  async spawn(root, _ctx, flags) {
    let binary = which("bash-language-server")
    const args: string[] = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("bash-language-server")
      if (!resolved) return
      binary = resolved
    }
    args.push("start")
    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
    }
  },
}

export const TerraformLS: Info = {
  id: "terraform",
  extensions: [".tf", ".tfvars"],
  root: NearestRoot([".terraform.lock.hcl", "terraform.tfstate", "*.tf"]),
  async spawn(root, _ctx, flags) {
    let bin = which("terraform-ls")

    if (!bin) {
      if (flags.disableLspDownload) return

      const releaseResponse = await fetch("https://api.releases.hashicorp.com/v1/releases/terraform-ls/latest")
      if (!releaseResponse.ok) {
        return
      }

      const release = (await releaseResponse.json()) as {
        version?: string
        builds?: { arch?: string; os?: string; url?: string }[]
      }

      const platform = process.platform
      const arch = process.arch

      const tfArch = arch === "arm64" ? "arm64" : "amd64"
      const tfPlatform = platform === "win32" ? "windows" : platform

      const builds = release.builds ?? []
      const build = builds.find((b) => b.arch === tfArch && b.os === tfPlatform)
      if (!build?.url) {
        return
      }

      const downloadResponse = await fetch(build.url)
      if (!downloadResponse.ok) {
        return
      }

      const tempPath = path.join(Global.Path.bin, "terraform-ls.zip")
      if (downloadResponse.body) await Filesystem.writeStream(tempPath, downloadResponse.body)

      const ok = await Archive.extractZip(tempPath, Global.Path.bin)
        .then(() => true)
        .catch((error) => {
          return false
        })
      if (!ok) return
      await fs.rm(tempPath, { force: true })

      bin = path.join(Global.Path.bin, "terraform-ls" + (platform === "win32" ? ".exe" : ""))

      if (!(await Filesystem.exists(bin))) {
        return
      }

      if (platform !== "win32") {
        await fs.chmod(bin, 0o755).catch(() => {})
      }
    }

    const initialization = {
      experimentalFeatures: {
        prefillRequiredFields: true,
        validateOnSave: true,
      },
    }
    const canonical = await canonicalRoot(root)
    const executable = await fs.realpath(bin).catch(() => bin)
    const key = `${executable}\0${JSON.stringify(initialization)}`
    const endpoint = terraformEndpointForKey(key)
    let broker = terraformBrokers.get(key)
    const lockPath = lockPathForKey(key)
    let owner = false
    if (!broker) {
      owner = await fs.writeFile(lockPath, String(process.pid), { flag: "wx", mode: 0o600 }).then(() => true).catch(() => false)
      if (!owner) {
        for (let attempt = 0; attempt < 40; attempt++) {
          broker = terraformBrokers.get(key)
          if (broker) break
          const discovered = await fs.readFile(endpoint, "utf8").then((value) => JSON.parse(value) as { key?: string; socket?: string; secret?: string; pid?: number; workerPid?: number }).catch(() => undefined)
          if (discovered?.key === key && discovered.socket && discovered.secret && discovered.pid && discovered.workerPid) break
          await new Promise((resolve) => setTimeout(resolve, 25))
        }
      }
    }
    if (broker) {
      const uri = pathToFileURL(canonical).href
      const count = broker.roots.get(uri) ?? 0
      broker.roots.set(uri, count + 1)
      const attached = attachTerraformClient(broker, canonical)
      const virtual = {
        stdin: attached.input, stdout: attached.output, stderr: new PassThrough(), pid: broker.process.pid,
        exitCode: null, signalCode: null,
        kill() { if (attached.client.closed) return; deactivateTerraformProcess(virtual); attached.client.closed = true; attached.input.end(); attached.output.end(); broker!.clients.delete(attached.client); const leases = (broker!.roots.get(uri) ?? 1) - 1; if (leases <= 0) broker!.roots.delete(uri); else broker!.roots.set(uri, leases); if (!broker!.clients.size) void Process.stop(broker!.process) },
        exited: new Promise<number>((resolve) => broker!.process.once("exit", (code) => resolve(code ?? 0))),
      }
      terraformProcessStatus.set(virtual, { mode: "pooled", rootFingerprint: canonical, workerPID: broker.process.pid, active: true })
      return { process: virtual as unknown as ChildProcessWithoutNullStreams, initialization }
    }
    const discovered = await fs.readFile(endpoint, "utf8").then((value) => JSON.parse(value) as { key?: string; socket?: string; secret?: string; pid?: number; workerPid?: number }).catch(() => undefined)
    const secureEndpoint = discovered && await fs.stat(endpoint).then((stat) => (stat.mode & 0o077) === 0).catch(() => false)
    const liveOwner = secureEndpoint && discovered?.pid ? (() => { try { process.kill(discovered.pid!, 0); return true } catch { return false } })() : false
    if (!owner && liveOwner && discovered?.key === key && discovered.socket && discovered.secret && discovered.pid && discovered.workerPid) {
      const { socket } = await open(discovered.socket, discovered.secret, canonical)
      const virtual = {
        stdin: socket,
        stdout: socket,
        stderr: new PassThrough(),
        pid: discovered.workerPid,
        exitCode: null,
        signalCode: null,
        kill() { deactivateTerraformProcess(virtual); socket.destroy() },
        exited: new Promise<number>((resolve) => socket.once("close", () => resolve(0))),
      }
      terraformProcessStatus.set(virtual, { mode: "pooled", rootFingerprint: canonical, workerPID: discovered.workerPid, active: true })
      return { process: virtual as unknown as ChildProcessWithoutNullStreams, initialization }
    }
    if (!broker) {
      const starting = terraformStarting.get(key)
      if (starting) broker = (await starting)?.broker
      else {
        const startup = (async () => {
          const child = spawn(bin, ["serve"], { cwd: os.homedir() })
          if (!(await terraformSupportsMultiRoot(child, canonical, initialization))) {
            await Process.stop(child)
            await fs.rm(lockPath, { force: true })
            return undefined
          }
          const connected = await connectTerraformBroker(key, canonical, child).catch(async () => {
            await Process.stop(child)
            return undefined
          })
          if (!connected?.broker) {
            await fs.rm(lockPath, { force: true })
            return undefined
          }
          child.on("exit", () => terraformBrokers.delete(key))
          return { broker: connected.broker, connection: connected, child }
        })()
        terraformStarting.set(key, startup)
        const started = await startup
        terraformStarting.delete(key)
        if (!started) return terraformDirect(bin, root, initialization, canonical)
        broker = started.broker
        const virtual = {
          stdin: started.connection.socket, stdout: started.connection.socket, stderr: new PassThrough(), pid: started.child.pid,
          exitCode: null, signalCode: null, kill() { deactivateTerraformProcess(virtual); started.connection.socket.destroy() },
          exited: new Promise<number>((resolve) => started.child.once("exit", (code) => resolve(code ?? 0))),
        }
        terraformProcessStatus.set(virtual, { mode: "pooled", rootFingerprint: canonical, workerPID: started.child.pid, active: true })
        return { process: virtual as unknown as ChildProcessWithoutNullStreams, initialization }
      }
    }
    if (!broker) return terraformDirect(bin, root, initialization, canonical)
    const uri = pathToFileURL(canonical).href
    const count = broker.roots.get(uri) ?? 0
    broker.roots.set(uri, count + 1)
    const attached = attachTerraformClient(broker, canonical)
    const virtual = {
      stdin: attached.input,
      stdout: attached.output,
      stderr: new PassThrough(),
      pid: broker.process.pid,
      exitCode: null,
      signalCode: null,
      kill() {
        if (attached.client.closed) return
        deactivateTerraformProcess(virtual)
        attached.client.closed = true
        attached.client.input.end()
        attached.client.output.end()
        broker!.clients.delete(attached.client)
        const leases = (broker!.roots.get(uri) ?? 1) - 1
        if (leases <= 0) {
          broker!.roots.delete(uri)
          if (broker!.initialized) broker!.process.stdin.write(frame({ jsonrpc: "2.0", method: "workspace/didChangeWorkspaceFolders", params: { event: { added: [], removed: [{ name: "workspace", uri }] } } }))
        } else broker!.roots.set(uri, leases)
        if (!broker!.clients.size) {
          void Process.stop(broker!.process)
          const ipc = terraformIPC.get(key)
          ipc?.server.close()
          terraformIPC.delete(key)
          cleanupTerraformBroker(key, broker!, undefined, endpoint)
        }
      },
      exited: new Promise<number>((resolve) => broker!.process.once("exit", (code) => resolve(code ?? 0))),
    }
    terraformProcessStatus.set(virtual, { mode: "pooled", rootFingerprint: canonical, workerPID: broker.process.pid, active: true })
    if (count === 0 && broker.initialized) {
      broker.process.stdin.write(frame({ jsonrpc: "2.0", method: "workspace/didChangeWorkspaceFolders", params: { event: { added: [{ name: "workspace", uri }], removed: [] } } }))
    }
    return { process: virtual as unknown as ChildProcessWithoutNullStreams, initialization }
  },
}

export const TexLab: Info = {
  id: "texlab",
  extensions: [".tex", ".bib"],
  root: NearestRoot([".latexmkrc", "latexmkrc", ".texlabroot", "texlabroot"]),
  async spawn(root, _ctx, flags) {
    let bin = which("texlab")

    if (!bin) {
      if (flags.disableLspDownload) return

      const response = await fetch("https://api.github.com/repos/latex-lsp/texlab/releases/latest")
      if (!response.ok) {
        return
      }

      const release = (await response.json()) as {
        tag_name?: string
        assets?: { name?: string; browser_download_url?: string }[]
      }
      const version = release.tag_name?.replace("v", "")
      if (!version) {
        return
      }

      const platform = process.platform
      const arch = process.arch

      const texArch = arch === "arm64" ? "aarch64" : "x86_64"
      const texPlatform = platform === "darwin" ? "macos" : platform === "win32" ? "windows" : "linux"
      const ext = platform === "win32" ? "zip" : "tar.gz"
      const assetName = `texlab-${texArch}-${texPlatform}.${ext}`

      const assets = release.assets ?? []
      const asset = assets.find((a) => a.name === assetName)
      if (!asset?.browser_download_url) {
        return
      }

      const downloadResponse = await fetch(asset.browser_download_url)
      if (!downloadResponse.ok) {
        return
      }

      const tempPath = path.join(Global.Path.bin, assetName)
      if (downloadResponse.body) await Filesystem.writeStream(tempPath, downloadResponse.body)

      if (ext === "zip") {
        const ok = await Archive.extractZip(tempPath, Global.Path.bin)
          .then(() => true)
          .catch((error) => {
            return false
          })
        if (!ok) return
      }
      if (ext === "tar.gz") {
        await run(["tar", "-xzf", tempPath], { cwd: Global.Path.bin })
      }

      await fs.rm(tempPath, { force: true })

      bin = path.join(Global.Path.bin, "texlab" + (platform === "win32" ? ".exe" : ""))

      if (!(await Filesystem.exists(bin))) {
        return
      }

      if (platform !== "win32") {
        await fs.chmod(bin, 0o755).catch(() => {})
      }
    }

    return {
      process: spawn(bin, {
        cwd: root,
      }),
    }
  },
}

export const DockerfileLS: Info = {
  id: "dockerfile",
  extensions: [".dockerfile", "Dockerfile"],
  root: async (_file, ctx) => ctx.directory,
  async spawn(root, _ctx, flags) {
    let binary = which("docker-langserver")
    const args: string[] = []
    if (!binary) {
      if (flags.disableLspDownload) return
      const resolved = await Npm.which("dockerfile-language-server-nodejs")
      if (!resolved) return
      binary = resolved
    }
    args.push("--stdio")
    const proc = spawn(binary, args, {
      cwd: root,
      env: {
        ...process.env,
      },
    })
    return {
      process: proc,
    }
  },
}

export const Gleam: Info = {
  id: "gleam",
  extensions: [".gleam"],
  root: NearestRoot(["gleam.toml"]),
  async spawn(root) {
    const gleam = which("gleam")
    if (!gleam) {
      return
    }
    return {
      process: spawn(gleam, ["lsp"], {
        cwd: root,
      }),
    }
  },
}

export const Clojure: Info = {
  id: "clojure-lsp",
  extensions: [".clj", ".cljs", ".cljc", ".edn"],
  root: NearestRoot(["deps.edn", "project.clj", "shadow-cljs.edn", "bb.edn", "build.boot"]),
  async spawn(root) {
    let bin = which("clojure-lsp")
    if (!bin && process.platform === "win32") {
      bin = which("clojure-lsp.exe")
    }
    if (!bin) {
      return
    }
    return {
      process: spawn(bin, ["listen"], {
        cwd: root,
      }),
    }
  },
}

export const Nixd: Info = {
  id: "nixd",
  extensions: [".nix"],
  root: async (file, ctx) => {
    // First, look for flake.nix - the most reliable Nix project root indicator
    const flakeRoot = await NearestRoot(["flake.nix"])(file, ctx)
    if (flakeRoot && flakeRoot !== ctx.directory) return flakeRoot

    // If no flake.nix, fall back to git repository root
    if (ctx.worktree && ctx.worktree !== ctx.directory) return ctx.worktree

    // Finally, use the instance directory as fallback
    return ctx.directory
  },
  async spawn(root) {
    const nixd = which("nixd")
    if (!nixd) {
      return
    }
    return {
      process: spawn(nixd, [], {
        cwd: root,
        env: {
          ...process.env,
        },
      }),
    }
  },
}

export const Tinymist: Info = {
  id: "tinymist",
  extensions: [".typ", ".typc"],
  root: NearestRoot(["typst.toml"]),
  async spawn(root, _ctx, flags) {
    let bin = which("tinymist")

    if (!bin) {
      if (flags.disableLspDownload) return

      const response = await fetch("https://api.github.com/repos/Myriad-Dreamin/tinymist/releases/latest")
      if (!response.ok) {
        return
      }

      const release = (await response.json()) as {
        tag_name?: string
        assets?: { name?: string; browser_download_url?: string }[]
      }

      const platform = process.platform
      const arch = process.arch

      const tinymistArch = arch === "arm64" ? "aarch64" : "x86_64"
      let tinymistPlatform: string
      let ext: string

      if (platform === "darwin") {
        tinymistPlatform = "apple-darwin"
        ext = "tar.gz"
      } else if (platform === "win32") {
        tinymistPlatform = "pc-windows-msvc"
        ext = "zip"
      } else {
        tinymistPlatform = "unknown-linux-gnu"
        ext = "tar.gz"
      }

      const assetName = `tinymist-${tinymistArch}-${tinymistPlatform}.${ext}`

      const assets = release.assets ?? []
      const asset = assets.find((a) => a.name === assetName)
      if (!asset?.browser_download_url) {
        return
      }

      const downloadResponse = await fetch(asset.browser_download_url)
      if (!downloadResponse.ok) {
        return
      }

      const tempPath = path.join(Global.Path.bin, assetName)
      if (downloadResponse.body) await Filesystem.writeStream(tempPath, downloadResponse.body)

      if (ext === "zip") {
        const ok = await Archive.extractZip(tempPath, Global.Path.bin)
          .then(() => true)
          .catch((error) => {
            return false
          })
        if (!ok) return
      } else {
        await run(["tar", "-xzf", tempPath, "--strip-components=1"], { cwd: Global.Path.bin })
      }

      await fs.rm(tempPath, { force: true })

      bin = path.join(Global.Path.bin, "tinymist" + (platform === "win32" ? ".exe" : ""))

      if (!(await Filesystem.exists(bin))) {
        return
      }

      if (platform !== "win32") {
        await fs.chmod(bin, 0o755).catch(() => {})
      }
    }

    return {
      process: spawn(bin, { cwd: root }),
    }
  },
}

export const HLS: Info = {
  id: "haskell-language-server",
  extensions: [".hs", ".lhs"],
  root: NearestRoot(["stack.yaml", "cabal.project", "hie.yaml", "*.cabal"]),
  async spawn(root) {
    const bin = which("haskell-language-server-wrapper")
    if (!bin) {
      return
    }
    return {
      process: spawn(bin, ["--lsp"], {
        cwd: root,
      }),
    }
  },
}

export const JuliaLS: Info = {
  id: "julials",
  extensions: [".jl"],
  root: NearestRoot(["Project.toml", "Manifest.toml", "*.jl"]),
  async spawn(root) {
    const julia = which("julia")
    if (!julia) {
      return
    }
    return {
      process: spawn(julia, ["--startup-file=no", "--history-file=no", "-e", "using LanguageServer; runserver()"], {
        cwd: root,
      }),
    }
  },
}
