import { Context, Effect, Layer, SynchronizedRef } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"

export type ShellStatus = "running" | "completed" | "error" | "cancelled"

export type ShellSession = {
  id: string
  command: string
  description: string
  cwd: string
  workdir: string | undefined
  startedAt: number
  completedAt?: number
  status: ShellStatus
  output: string
  exit: number | null
  truncated: boolean
  outputPath?: string
  error?: string
}

export interface Interface {
  readonly create: (input: {
    id: string
    command: string
    description: string
    cwd: string
    workdir: string | undefined
  }) => Effect.Effect<ShellSession>
  readonly update: (id: string, patch: Partial<ShellSession>) => Effect.Effect<void>
  readonly get: (id: string) => Effect.Effect<ShellSession | undefined>
  readonly list: () => Effect.Effect<ShellSession[]>
  readonly delete: (id: string) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ShellSession") {}

export const make = Effect.gen(function* () {
  const ref = yield* SynchronizedRef.make(new Map<string, ShellSession>())

  const create: Interface["create"] = Effect.fn("ShellSessions.create")(function* (input) {
    const session: ShellSession = {
      id: input.id,
      command: input.command,
      description: input.description,
      cwd: input.cwd,
      workdir: input.workdir,
      startedAt: Date.now(),
      status: "running",
      output: "",
      exit: null,
      truncated: false,
    }
    yield* SynchronizedRef.update(ref, (map) => new Map(map).set(session.id, session))
    return session
  })

  const update: Interface["update"] = Effect.fn("ShellSessions.update")(function* (id, patch) {
    yield* SynchronizedRef.update(ref, (map) => {
      const existing = map.get(id)
      if (!existing) return map
      return new Map(map).set(id, { ...existing, ...patch, id: existing.id })
    })
  })

  const get: Interface["get"] = Effect.fn("ShellSessions.get")(function* (id) {
    return (yield* SynchronizedRef.get(ref)).get(id)
  })

  const list: Interface["list"] = Effect.fn("ShellSessions.list")(function* () {
    return Array.from((yield* SynchronizedRef.get(ref)).values()).toSorted(
      (a, b) => b.startedAt - a.startedAt,
    )
  })

  const remove: Interface["delete"] = Effect.fn("ShellSessions.delete")(function* (id) {
    yield* SynchronizedRef.update(ref, (map) => {
      if (!map.has(id)) return map
      const next = new Map(map)
      next.delete(id)
      return next
    })
  })

  return Service.of({ create, update, get, list, delete: remove })
})

/** Per-project shell session store; cleans up with the instance scope. */
export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const state = yield* InstanceState.make(() => make)
    return Service.of({
      create: (input) => InstanceState.useEffect(state, (sessions) => sessions.create(input)),
      update: (id, patch) => InstanceState.useEffect(state, (sessions) => sessions.update(id, patch)),
      get: (id) => InstanceState.useEffect(state, (sessions) => sessions.get(id)),
      list: () => InstanceState.useEffect(state, (sessions) => sessions.list()),
      delete: (id) => InstanceState.useEffect(state, (sessions) => sessions.delete(id)),
    })
  }),
)

export const defaultLayer = layer

export const node = LayerNode.make(layer, [])

export * as ShellSessions from "./sessions"
