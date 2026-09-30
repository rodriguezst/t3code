import { useAtomValue } from "@effect/atom-react";
import { createScheduledSendEnvironmentAtoms } from "@t3tools/client-runtime/state/scheduled-sends";
import type { EnvironmentId, ScheduledSendSnapshot } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";
import { serverEnvironment, environmentServerConfigsAtom } from "./server";

export const scheduledSendEnvironment = createScheduledSendEnvironmentAtoms(connectionAtomRuntime);
const EMPTY_SENDS: ReadonlyArray<ScheduledSendSnapshot> = [];
const emptyAtom = Atom.make(EMPTY_SENDS);
const scheduledSendsAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get) => {
    if (
      get(serverEnvironment.configValueAtom(environmentId))?.environment.capabilities
        .scheduledSends !== true
    )
      return EMPTY_SENDS;
    const result = get(scheduledSendEnvironment.list({ environmentId, input: {} }));
    if (result._tag === "Failure") return EMPTY_SENDS;
    return Option.getOrElse(AsyncResult.value(result), () => EMPTY_SENDS);
  }),
);

export function useScheduledSends(environmentId: EnvironmentId | null) {
  return useAtomValue(environmentId === null ? emptyAtom : scheduledSendsAtom(environmentId));
}

const allScheduledSendsAtom = Atom.make((get) =>
  Array.from(get(environmentServerConfigsAtom).entries()).flatMap(([environmentId, config]) =>
    get(scheduledSendsAtom(environmentId)).map((task) => ({
      environmentId,
      environmentLabel: config.environment.label,
      task,
    })),
  ),
);

export function useAllScheduledSends() {
  return useAtomValue(allScheduledSendsAtom);
}
