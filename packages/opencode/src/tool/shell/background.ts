import { Cause, Effect } from "effect"
import { containsPath } from "../../project/instance-context"
import { InstanceState } from "@/effect/instance-state"
import { Config } from "@/config/config"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Plugin } from "@/plugin"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Shell } from "@opencode-ai/core/shell"
import { BackgroundJob } from "@/background/job"
import { ShellSessions, type ShellSession } from "./sessions"
import { BackgroundToolID } from "./id"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import { ShellPrompt, type BackgroundParameters } from "./prompt"
import * as Tool from "../tool"
import * as Truncate from "../truncate"

import { ask, collect, parse, resolvePath, run, shellEnv } from "./internal"

export { BackgroundParameters as Parameters } from "./prompt"

export const ShellBackgroundTool = Tool.define(
  BackgroundToolID,
  Effect.gen(function* () {
    const config = yield* Config.Service
    const spawner = yield* ChildProcessSpawner
    const fs = yield* FSUtil.Service
    const trunc = yield* Truncate.Service
    const plugin = yield* Plugin.Service
    const flags = yield* RuntimeFlags.Service
    const background = yield* BackgroundJob.Service
    const sessions = yield* ShellSessions.Service
    const defaultTimeoutMs = flags.bashDefaultTimeoutMs ?? 2 * 60 * 1000

    const viewSession = Effect.fn("ShellBackgroundTool.view")(function* (id: string) {
      const session = yield* sessions.get(id)
      if (!session) {
        return yield* Effect.fail(new Error(`Unknown background shell session: ${id}`))
      }
      const head = session.output || "(no output)"
      const elapsed = Date.now() - session.startedAt
      const lines = [
        `<shell_session id="${session.id}" status="${session.status}">`,
        `<started_at>${new Date(session.startedAt).toISOString()}</started_at>`,
        `<elapsed_ms>${elapsed}</elapsed_ms>`,
        ...(session.workdir ? [`<workdir>${session.workdir}</workdir>`] : []),
        `<command>${session.command}</command>`,
        `<description>${session.description}</description>`,
        ...(session.status === "completed" || session.status === "error" || session.status === "cancelled"
          ? [`<exit>${session.exit ?? "null"}</exit>`]
          : []),
        ...(session.error ? [`<error>${session.error}</error>`] : []),
        ...(session.outputPath ? [`<output_path>${session.outputPath}</output_path>`] : []),
        `<output>`,
        head,
        `</output>`,
        `</shell_session>`,
      ]
      return {
        title: session.description,
        metadata: {
          sessionId: session.id,
          status: session.status,
          exit: session.exit,
          description: session.description,
          truncated: session.truncated,
          ...(session.outputPath ? { outputPath: session.outputPath } : {}),
        },
        output: lines.join("\n"),
      }
    })

    return () =>
      Effect.gen(function* () {
        const cfg = yield* config.get()
        const shell = Shell.acceptable(cfg.shell)
        const name = Shell.name(shell)
        const prompt = ShellPrompt.renderBackground(name, process.platform)
        yield* Effect.logInfo("shell background tool using shell", { shell })

        return {
          description: prompt.description,
          parameters: prompt.parameters,
          execute: (params: BackgroundParameters, ctx: Tool.Context) =>
            Effect.gen(function* () {
              if (params.session_id) {
                if (params.command) {
                  return yield* Effect.fail(
                    new Error("Provide either session_id (to view) or command/description (to start), not both"),
                  )
                }
                return (yield* viewSession(params.session_id)) as Tool.ExecuteResult
              }

              if (!params.command || !params.description) {
                return yield* Effect.fail(
                  new Error("command and description are required when starting a new background shell"),
                )
              }

              const instanceCtx = yield* InstanceState.context
              const cwd = params.workdir
                ? yield* resolvePath(params.workdir, instanceCtx.directory, shell, spawner)
                : instanceCtx.directory
              if (params.timeout !== undefined && params.timeout < 0) {
                throw new Error(`Invalid timeout value: ${params.timeout}. Timeout must be a positive number.`)
              }
              const timeout = params.timeout ?? defaultTimeoutMs
              const ps = Shell.ps(shell)
              const command = params.command
              const description = params.description
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const tree = yield* Effect.acquireRelease(parse(command, ps), (tree) =>
                    Effect.sync(() => tree.delete()),
                  )
                  const scan = yield* collect(tree.rootNode, cwd, ps, shell, instanceCtx, { fs, spawner })
                  if (!containsPath(cwd, instanceCtx)) scan.dirs.add(cwd)
                  yield* ask(ctx, scan, { command, description })
                }),
              )

              const env = yield* shellEnv(ctx, cwd, plugin)

              const sessionId = `shell_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`
              const initial = yield* sessions.create({
                id: sessionId,
                command,
                description,
                cwd,
                workdir: params.workdir,
              })

              // Background commands must survive the original tool call's abort signal,
              // so use a never-aborting signal here. Cancellation flows through
              // BackgroundJob.cancel instead, keyed by the returned sessionId.
              const backgroundAbort = new AbortController().signal
              const result = yield* background.start({
                id: sessionId,
                type: "bash",
                title: description,
                metadata: {
                  parentSessionId: ctx.sessionID,
                  sessionId,
                  command,
                  workdir: params.workdir,
                },
                run: run(
                  {
                    shell,
                    command,
                    cwd,
                    env,
                    timeout,
                    description,
                  },
                  ctx,
                  { spawner, trunc },
                  {
                    abort: backgroundAbort,
                    onChunk: ({ output, description }) =>
                      sessions.update(sessionId, { output, description }).pipe(Effect.asVoid),
                  },
                ).pipe(
                  Effect.matchCauseEffect({
                    onSuccess: (res) =>
                      Effect.gen(function* () {
                        yield* sessions.update(sessionId, {
                          status: res.exit !== null ? "completed" : "error",
                          output: res.preview,
                          exit: res.exit,
                          truncated: res.truncated,
                          ...(res.outputPath ? { outputPath: res.outputPath } : {}),
                          completedAt: Date.now(),
                        })
                        return res.output
                      }),
                    onFailure: (cause) =>
                      Effect.gen(function* () {
                        const isInterrupt = Cause.hasInterruptsOnly(cause)
                        yield* sessions.update(sessionId, {
                          status: isInterrupt ? "cancelled" : "error",
                          error: Cause.pretty(cause),
                          completedAt: Date.now(),
                        })
                        return yield* Effect.failCause(cause)
                      }),
                  }),
                ),
              })

              const final = (yield* sessions.get(sessionId)) ?? initial
              const summary = final.status === "running" ? "Background shell started" : "Background shell completed"
              const lines = [
                `<shell_session id="${final.id}" status="${final.status}">`,
                `<command>${final.command}</command>`,
                `<description>${final.description}</description>`,
                ...(final.workdir ? [`<workdir>${final.workdir}</workdir>`] : []),
                ...(final.status !== "running"
                  ? [`<exit>${final.exit ?? "null"}</exit>`]
                  : [
                      `<note>Pass session_id="${final.id}" back to ${BackgroundToolID} (with no other arguments) to view the current output and progress of this background command.</note>`,
                    ]),
                ...(final.error ? [`<error>${final.error}</error>`] : []),
                ...(final.outputPath ? [`<output_path>${final.outputPath}</output_path>`] : []),
                ...(final.status === "running" ? [`<output>${final.output || "(no output yet)"}</output>`] : []),
                `</shell_session>`,
              ]
              return {
                title: description,
                metadata: {
                  sessionId: result.id,
                  status: final.status,
                  description,
                  truncated: final.truncated,
                  ...(final.outputPath ? { outputPath: final.outputPath } : {}),
                  ...(final.status !== "running" ? { exit: final.exit } : {}),
                },
                output: [summary, ...lines].join("\n"),
              }
            }).pipe(Effect.orDie),
        }
      })
  }),
)
