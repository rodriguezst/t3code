import { WS_METHODS } from "@t3tools/contracts";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

export function createScheduledSendEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    create: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:scheduled-send:create",
      tag: WS_METHODS.scheduledSendCreate,
    }),
    cancel: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:scheduled-send:cancel",
      tag: WS_METHODS.scheduledSendCancel,
    }),
    list: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:scheduled-sends",
      tag: WS_METHODS.subscribeScheduledSends,
    }),
  };
}

/** Countdown labels deliberately use minutes, avoiding continuous UI work. */
export function scheduledSendCountdown(scheduledAt: string, now: number): string {
  const remaining = Date.parse(scheduledAt) - now;
  if (remaining <= 0) return "Due now";
  const minutes = Math.ceil(remaining / 60_000);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  if (days > 0) return `In ${days}d ${hours % 24}h`;
  if (hours > 0) return `In ${hours}h ${minutes % 60}m`;
  return `In ${minutes}m`;
}
