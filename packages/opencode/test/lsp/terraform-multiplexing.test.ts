import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "bun:test"
import { spawn } from "child_process"
import { once } from "events"
import fs from "fs/promises"
import path from "path"
import { pathToFileURL } from "url"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect, Layer } from "effect"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { LSPClient } from "@/lsp/client"
import { LSP } from "@/lsp/lsp"
import * as LSPServer from "@/lsp/server"
import { TestInstance, tmpdir, withTestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const fixture = path.join(__dirname, "../fixture/lsp/fake-lsp-server.js")
const originalPath = process.env.PATH
const lspLayer = LayerNode.compile(LayerNode.group([LSP.node, Config.node, RuntimeFlags.node, EventV2Bridge.node]), [
  [RuntimeFlags.node, RuntimeFlags.layer({})],
])
const it = testEffect(Layer.mergeAll(lspLayer, LayerNode.compile(CrossSpawnSpawner.node)))

type TerraformPoolStatus = {
  mode: "pooled" | "direct" | "disabled"
  rootFingerprint?: string
  workerPID?: number
  active: boolean
}

// AC 69, AC 71, and AC 73: this is intentionally an internal, read-only
// service seam. Its cast keeps the red test executable until the seam exists.
function terraformPoolStatus(lsp: LSP.Interface) {
  return (lsp as LSP.Interface & { terraformPoolStatus: () => Effect.Effect<TerraformPoolStatus[]> }).terraformPoolStatus()
}

async function fakeTerraform(binary: string, compatible = true) {
  const server = `${binary}.js`
  let source = await Bun.file(fixture).text()
  source = source.replace(
    '  if (data.method === "test/get-last-change") {',
    `  if (data.method === "test/worker-publish-diagnostics") {
    sendNotification("textDocument/publishDiagnostics", {
      uri: data.params?.target,
      diagnostics: data.params?.diagnostics ?? [],
    })
    return
  }

  if (data.method === "test/get-last-change") {`,
  )
  source = source.replace(
    '  if (data.method === "test/get-last-change") {',
    `  if (data.method === "test/workspace-result") {
    sendResponse(data.id, [{ location: { uri: data.params?.target, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } } } }])
    return
  }

  if (data.method === "test/get-last-change") {`,
  )
  if (compatible) {
    source = source.replace(
      "capabilities: {\n        textDocumentSync:",
      "capabilities: {\n        workspace: { workspaceFolders: { supported: true, changeNotifications: true } },\n        textDocumentSync:",
    )
  }
  await Bun.write(server, source)
  await Bun.write(binary, `#!/bin/sh\nexec "${process.execPath}" "${server}" "$@"\n`)
  await fs.chmod(binary, 0o755)
}

afterEach(() => {
  if (originalPath === undefined) delete process.env.PATH
  else process.env.PATH = originalPath
})

describe("Terraform built-in LSP multiplexing", () => {
  test("AC 74: compatible roots initialize exactly one shared multi-root worker", async () => {
    await using binaries = await tmpdir()
    await using first = await tmpdir()
    await using second = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")

    await fakeTerraform(binary)
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    const clients = await Promise.all(
      [first.path, second.path].map((root) =>
        withTestInstance({
          directory: root,
          fn: async (instance) => {
            const server = await LSPServer.TerraformLS.spawn(root, instance, {} as RuntimeFlags.Info)
            if (!server) throw new Error("fake terraform-ls was not discovered")
            return {
              client: await LSPClient.create({
                serverID: "terraform",
                server,
                root,
                directory: root,
                instance,
              }),
              pid: server.process.pid,
              process: server.process as typeof server.process & { exited: Promise<number> },
            }
          },
        }),
      ),
    )

    try {
      expect(new Set(clients.map((client) => client.pid)).size).toBe(1)
      const params = await clients[0]!.client.connection.sendRequest<any>("test/get-initialize-params", {})
      expect(params.rootUri).toBeNull()
      expect(params.workspaceFolders).toEqual(expect.arrayContaining([
        expect.objectContaining({ uri: expect.stringContaining(first.path) }),
        expect.objectContaining({ uri: expect.stringContaining(second.path) }),
      ]))
      await clients[0]!.client.shutdown()
      await clients[1]!.client.connection.sendRequest("test/get-initialize-params", {})
      await clients[1]!.client.shutdown()
      expect(await clients[0]!.process.exited).toBe(0)
    } finally {
      await Promise.all(clients.map((client) => client.client.shutdown()))
    }
  })

  test("AC 73 and AC 74: separate OpenCode processes converge on one user-scoped worker", async () => {
    await using binaries = await tmpdir()
    await using state = await tmpdir()
    await using first = await tmpdir()
    await using second = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")

    await fakeTerraform(binary)
    const worker = `
      import * as LSPServer from "./src/lsp/server"
      import { LSPClient } from "./src/lsp/client"
      import { Global } from "@opencode-ai/core/global"
      const handle = await LSPServer.TerraformLS.spawn(process.env.TERRAFORM_ROOT, { directory: process.env.TERRAFORM_ROOT }, {})
      if (!handle) process.exit(2)
      const client = await LSPClient.create({
        serverID: "terraform",
        server: handle,
        root: process.env.TERRAFORM_ROOT,
        directory: process.env.TERRAFORM_ROOT,
        instance: { directory: process.env.TERRAFORM_ROOT },
      })
      process.stdout.write(JSON.stringify({ pid: handle.process.pid, state: Global.Path.state }) + "\\n")
      process.stdin.once("data", async () => {
        await client.shutdown()
        await handle.process.exited
        process.exit(0)
      })
    `
    const start = (root: string) => {
      const child = spawn(process.execPath, ["-e", worker], {
        cwd: process.cwd(),
        env: {
          ...process.env,
          PATH: `${binaries.path}${path.delimiter}${originalPath ?? ""}`,
          TERRAFORM_ROOT: root,
          XDG_STATE_HOME: state.path,
        },
        stdio: ["pipe", "pipe", "pipe"],
      })
      return child
    }
    const workers = [start(first.path), start(second.path)]

    let started: { pid: number; state: string }[] = []
    try {
      started = await Promise.all(workers.map(async (worker) => {
        const [chunk] = await once(worker.stdout!, "data")
        return JSON.parse(String(chunk)) as { pid: number; state: string }
      }))
      expect(new Set(started.map((worker) => worker.pid)).size).toBe(1)
      const statePath = started[0]!.state
      const entries = await fs.readdir(statePath)
      const artifacts = entries.filter((entry) => entry.startsWith("terraform-lsp-"))
      expect(artifacts).toEqual(expect.arrayContaining([
        expect.stringMatching(/^terraform-lsp-broker-[a-f0-9]+\.json$/),
        expect.stringMatching(/^terraform-lsp-[a-f0-9]+\.sock$/),
      ]))
      for (const entry of artifacts) {
        expect((await fs.stat(path.join(statePath, entry))).mode & 0o077).toBe(0)
      }
    } finally {
      for (const worker of workers) worker.stdin?.write("close\n")
      await Promise.all(workers.map((worker) => once(worker, "exit")))
    }
    const remaining = await fs.readdir(started[0]!.state).catch(() => [])
    expect(remaining.filter((entry) => entry.startsWith("terraform-lsp-"))).toEqual([])
  })

  test("AC 19 and AC 25: distinct built-in fingerprints retain their existing workers", async () => {
    await using firstBinary = await tmpdir()
    await using secondBinary = await tmpdir()
    await using state = await tmpdir()
    await using firstRoot = await tmpdir()
    await using secondRoot = await tmpdir()
    await using thirdRoot = await tmpdir()

    await fakeTerraform(path.join(firstBinary.path, "terraform-ls"))
    await fakeTerraform(path.join(secondBinary.path, "terraform-ls"))
    const worker = `
      import * as LSPServer from "./src/lsp/server"
      import { LSPClient } from "./src/lsp/client"
      const handle = await LSPServer.TerraformLS.spawn(process.env.TERRAFORM_ROOT, { directory: process.env.TERRAFORM_ROOT }, {})
      if (!handle) process.exit(2)
      const client = await LSPClient.create({
        serverID: "terraform",
        server: handle,
        root: process.env.TERRAFORM_ROOT,
        directory: process.env.TERRAFORM_ROOT,
        instance: { directory: process.env.TERRAFORM_ROOT },
      })
      process.stdout.write(JSON.stringify({ type: "started", pid: handle.process.pid }) + "\\n")
      process.stdin.on("data", async (chunk) => {
        if (String(chunk).trim() === "probe") {
          await client.connection.sendRequest("test/get-initialize-params", {})
          process.stdout.write(JSON.stringify({ type: "probe" }) + "\\n")
          return
        }
        await client.shutdown()
        await handle.process.exited
        process.exit(0)
      })
    `
    const start = (root: string, binaries: string) =>
      spawn(process.execPath, ["-e", worker], {
        cwd: process.cwd(),
        env: {
          ...process.env,
          PATH: `${binaries}${path.delimiter}${originalPath ?? ""}`,
          TERRAFORM_ROOT: root,
          XDG_STATE_HOME: state.path,
        },
        stdio: ["pipe", "pipe", "pipe"],
      })
    const read = async (child: ReturnType<typeof start>) => {
      const [chunk] = await once(child.stdout!, "data")
      return JSON.parse(String(chunk)) as { type: "started" | "probe"; pid?: number }
    }
    const first = start(firstRoot.path, firstBinary.path)
    const second = start(secondRoot.path, secondBinary.path)
    let third: ReturnType<typeof start> | undefined

    try {
      const firstStarted = await read(first)
      const secondStarted = await read(second)
      expect(firstStarted.pid).not.toBe(secondStarted.pid)

      first.stdin!.write("probe\n")
      expect((await read(first)).type).toBe("probe")

      third = start(thirdRoot.path, firstBinary.path)
      const thirdStarted = await read(third)
      expect(thirdStarted.pid).toBe(firstStarted.pid)
    } finally {
      for (const child of [first, second, third].filter((child): child is ReturnType<typeof start> => !!child)) {
        child.stdin?.write("close\n")
      }
      await Promise.all([first, second, third].filter(Boolean).map((child) => once(child!, "exit")))
    }
  })

  test("AC 74 and AC 75: symlink aliases share one canonical-root worker", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    await using aliases = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")
    const alias = path.join(aliases.path, "workspace")

    await fakeTerraform(binary)
    await fs.symlink(root.path, alias)
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    const clients = await Promise.all(
      [root.path, alias].map((directory) =>
        withTestInstance({
          directory,
          fn: async (instance) => {
            const server = await LSPServer.TerraformLS.spawn(directory, instance, {} as RuntimeFlags.Info)
            if (!server) throw new Error("fake terraform-ls was not discovered")
            return {
              client: await LSPClient.create({
                serverID: "terraform",
                server,
                root: directory,
                directory,
                instance,
              }),
              pid: server.process.pid,
            }
          },
        }),
      ),
    )

    try {
      expect(new Set(clients.map((client) => client.pid)).size).toBe(1)
    } finally {
      await Promise.all(clients.map((client) => client.client.shutdown()))
    }
  })

  test("AC 74 and AC 76: the shared worker initializes without a global rootUri", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")

    await fakeTerraform(binary)
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({
          serverID: "terraform",
          server,
          root: root.path,
          directory: root.path,
          instance,
        })

        try {
          const params = await client.connection.sendRequest<any>("test/get-initialize-params", {})
          expect(params).toMatchObject({
            rootUri: null,
            workspaceFolders: [{ uri: expect.stringContaining(root.path) }],
            capabilities: {
              workspace: {
                workspaceFolders: true,
              },
            },
          })
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  test("AC 75: Terraform diagnostics for another root are not delivered to this client", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    await using outside = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")
    const foreign = path.join(outside.path, "foreign.tf")

    await fakeTerraform(binary)
    await Bun.write(foreign, "resource \"null_resource\" \"foreign\" {}\n")
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({
          serverID: "terraform",
          server,
          root: root.path,
          directory: root.path,
          instance,
        })

        try {
          await client.connection.sendNotification("test/publish-diagnostics", {
            uri: pathToFileURL(foreign).href,
            diagnostics: [{ message: "foreign root diagnostic", range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } } }],
          })
          await client.connection.sendRequest("test/get-initialize-params", {})
          expect(client.diagnostics.has(foreign)).toBe(false)
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  test("AC 75: a file URI that escapes through a symlink is rejected", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    await using outside = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")
    const escaped = path.join(root.path, "linked.tf")
    const foreign = path.join(outside.path, "foreign.tf")

    await fakeTerraform(binary)
    await Bun.write(foreign, "resource \"null_resource\" \"foreign\" {}\n")
    await fs.symlink(foreign, escaped)
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({
          serverID: "terraform",
          server,
          root: root.path,
          directory: root.path,
          instance,
        })

        try {
          await expect(client.connection.sendRequest("test/foreign-uri", { uri: pathToFileURL(escaped).href })).rejects.toThrow(
            "URI is outside the workspace root",
          )
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  test("AC 70: worker diagnostics through a symlink escape are not delivered", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    await using outside = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")
    const escaped = path.join(root.path, "linked.tf")
    const foreign = path.join(outside.path, "foreign.tf")

    await fakeTerraform(binary)
    await Bun.write(foreign, "resource \"null_resource\" \"foreign\" {}\n")
    await fs.symlink(foreign, escaped)
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({
          serverID: "terraform",
          server,
          root: root.path,
          directory: root.path,
          instance,
        })

        try {
          await client.connection.sendNotification("test/worker-publish-diagnostics", {
            target: pathToFileURL(escaped).href,
            diagnostics: [{ message: "escaped worker diagnostic", range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } } }],
          })
          await client.connection.sendRequest("test/get-last-change", {})
          expect(client.diagnostics.size).toBe(0)
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  test("AC 70: worker diagnostic related locations outside the root are filtered", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    await using outside = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")
    const file = path.join(root.path, "main.tf")
    const foreign = path.join(outside.path, "foreign.tf")

    await fakeTerraform(binary)
    await Bun.write(file, "terraform {}\n")
    await Bun.write(foreign, "resource \"null_resource\" \"foreign\" {}\n")
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({ serverID: "terraform", server, root: root.path, directory: root.path, instance })

        try {
          await client.connection.sendNotification("test/worker-publish-diagnostics", {
            target: pathToFileURL(file).href,
            diagnostics: [{
              message: "root diagnostic",
              range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } },
              relatedInformation: [{
                message: "foreign related diagnostic",
                location: { uri: pathToFileURL(foreign).href, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } } },
              }],
            }],
          })
          await client.connection.sendRequest("test/get-last-change", {})
          expect(client.diagnostics.get(file)?.[0]?.relatedInformation).toEqual([])
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  test("AC 70: worker workspace results outside the root are filtered", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    await using outside = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")
    const foreign = path.join(outside.path, "foreign.tf")

    await fakeTerraform(binary)
    await Bun.write(foreign, "resource \"null_resource\" \"foreign\" {}\n")
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({ serverID: "terraform", server, root: root.path, directory: root.path, instance })

        try {
          const result = await client.connection.sendRequest("test/workspace-result", { target: pathToFileURL(foreign).href })
          expect(result).toEqual([])
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  test("AC 75 and AC 76: worker capability registration reaches its owning client", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")
    const file = path.join(root.path, "main.tf")

    await fakeTerraform(binary)
    await Bun.write(file, "terraform {}\n")
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({
          serverID: "terraform",
          server,
          root: root.path,
          directory: root.path,
          instance,
        })

        try {
          await client.connection.sendRequest("test/configure-pull-diagnostics", {
            registrations: [{ identifier: "syntax" }],
            documentDiagnostics: [
              {
                range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } },
                message: "registered diagnostic",
                severity: 1,
              },
            ],
          })
          const version = await client.notify.open({ path: file })
          await client.connection.sendRequest("test/register-configured-pull-diagnostics", {})
          await client.waitForDiagnostics({ path: file, version, mode: "document" })
          expect(client.diagnostics.get(file)?.[0]?.message).toBe("registered diagnostic")
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  test("AC 76 and AC 78: a worker without dynamic workspace folders falls back to direct launch", async () => {
    await using binaries = await tmpdir()
    await using root = await tmpdir()
    const binary = path.join(binaries.path, "terraform-ls")

    // This dedicated mode omits workspace.workspaceFolders.
    await fakeTerraform(binary, false)
    process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`

    await withTestInstance({
      directory: root.path,
      fn: async (instance) => {
        const server = await LSPServer.TerraformLS.spawn(root.path, instance, {} as RuntimeFlags.Info)
        if (!server) throw new Error("fake terraform-ls was not discovered")
        const client = await LSPClient.create({
          serverID: "terraform",
          server,
          root: root.path,
          directory: root.path,
          instance,
        })

        try {
          const params = await client.connection.sendRequest<any>("test/get-initialize-params", {})
          expect(params.rootUri).toBe(pathToFileURL(root.path).href)
        } finally {
          await client.shutdown()
        }
      },
    })
  })

  it.instance(
    "AC 77: disabled Terraform does not launch a client",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const directory = (yield* TestInstance).directory
          const file = path.join(directory, "main.tf")
          yield* Effect.promise(() => Bun.write(file, "terraform {}\n"))
          yield* lsp.touchFile(file)
          expect(yield* lsp.status()).toEqual([])
          expect(yield* terraformPoolStatus(lsp)).toEqual([])
        }),
      ),
    { config: { lsp: { terraform: { disabled: true } } } },
  )

  it.instance(
    "AC 77: a custom Terraform command remains a direct root-bound client",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const directory = (yield* TestInstance).directory
          const file = path.join(directory, "main.tf")
          yield* Effect.promise(() => Bun.write(file, "terraform {}\n"))
          yield* lsp.touchFile(file)
          expect(yield* lsp.status()).toEqual([
            { id: "terraform", name: "terraform", root: "", status: "connected" },
          ])
        }),
      ),
    { config: { lsp: { terraform: { command: [process.execPath, fixture], extensions: [".tf"] } } } },
  )

  describe("configuration selection", () => {
    let binaries: Awaited<ReturnType<typeof tmpdir>>

    beforeAll(async () => {
      binaries = await tmpdir()
      await fakeTerraform(path.join(binaries.path, "terraform-ls"))
    })

    beforeEach(() => {
      process.env.PATH = `${binaries.path}${path.delimiter}${originalPath ?? ""}`
    })

    afterAll(async () => {
      await binaries[Symbol.asyncDispose]()
    })

    it.instance(
      "AC 69 and required test 4: lsp: true selects the built-in Terraform pool without an opt-in",
      () =>
        LSP.Service.use((lsp) =>
          Effect.gen(function* () {
            const directory = (yield* TestInstance).directory
            const file = path.join(directory, "main.tf")
            yield* Effect.promise(() => Bun.write(file, "terraform {}\n"))
            yield* lsp.touchFile(file)
            expect(yield* terraformPoolStatus(lsp)).toEqual([
              expect.objectContaining({ mode: "pooled", rootFingerprint: expect.any(String), workerPID: expect.any(Number), active: true }),
            ])
          }),
        ),
      { config: { lsp: true } },
    )

    it.instance(
      "AC 69 and required test 4: an override limited to another server leaves built-in Terraform pooled",
      () =>
        LSP.Service.use((lsp) =>
          Effect.gen(function* () {
            const directory = (yield* TestInstance).directory
            const file = path.join(directory, "main.tf")
            yield* Effect.promise(() => Bun.write(file, "terraform {}\n"))
            yield* lsp.touchFile(file)
            expect(yield* terraformPoolStatus(lsp)).toEqual([
              expect.objectContaining({ mode: "pooled", rootFingerprint: expect.any(String), workerPID: expect.any(Number), active: true }),
            ])
          }),
        ),
      { config: { lsp: { eslint: { disabled: true } } } },
    )

    for (const [name, terraform] of [
      ["environment", { command: [process.execPath, fixture], env: { TERRAFORM_TEST: "direct" } }],
      ["initialization", { command: [process.execPath, fixture], initialization: { test: true } }],
      ["extensions", { command: [process.execPath, fixture], extensions: [".tf"] }],
    ] as const) {
      it.instance(
        `AC 71 and required test 4: a Terraform ${name} override bypasses pooling and launches directly`,
        () =>
          LSP.Service.use((lsp) =>
            Effect.gen(function* () {
              const directory = (yield* TestInstance).directory
              const file = path.join(directory, "main.tf")
              yield* Effect.promise(() => Bun.write(file, "terraform {}\n"))
              yield* lsp.touchFile(file)
              expect(yield* terraformPoolStatus(lsp)).toEqual([
                expect.objectContaining({ mode: "direct", rootFingerprint: expect.any(String), active: true }),
              ])
            }),
          ),
        { config: { lsp: { terraform: { ...terraform, command: [...terraform.command], extensions: terraform.extensions && [...terraform.extensions] } } } },
      )
    }
  })
})
