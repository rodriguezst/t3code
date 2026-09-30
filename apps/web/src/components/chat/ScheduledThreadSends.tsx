import { scheduledSendCountdown } from "@t3tools/client-runtime/state/scheduled-sends";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Clock3Icon } from "lucide-react";
import { useEffect, useState } from "react";

import { useScheduledSends } from "../../state/scheduledSends";

export function ScheduledThreadSends({
  environmentId,
  threadId,
}: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  const sends = useScheduledSends(environmentId);
  const pending = sends.filter((send) => send.threadId === threadId && send.phase === "pending");
  const [now, setNow] = useState(Date.now);
  const hasPending = pending.length > 0;
  useEffect(() => {
    if (!hasPending) return;
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, [hasPending]);
  if (!hasPending) return null;
  return (
    <div className="flex flex-col gap-1 px-3 py-2 text-xs text-muted-foreground">
      {pending.map((send) => (
        <div key={send.id} className="flex items-center gap-2">
          <Clock3Icon className="size-3 shrink-0" />
          <span className="truncate">
            Scheduled send · {scheduledSendCountdown(send.scheduledAt, now)} ·{" "}
            {new Date(send.scheduledAt).toLocaleString()}
          </span>
        </div>
      ))}
    </div>
  );
}
