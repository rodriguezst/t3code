import { Link } from "@tanstack/react-router";
import { scheduledSendCountdown } from "@t3tools/client-runtime/state/scheduled-sends";
import { Clock3Icon, XIcon } from "lucide-react";
import { useEffect, useState } from "react";

import { buildThreadRouteParams } from "../threadRoutes";
import { scheduledSendEnvironment, useAllScheduledSends } from "../state/scheduledSends";
import { useEnvironments } from "../state/environments";
import { useAtomCommand } from "../state/use-atom-command";
import { toastManager } from "./ui/toast";
import { Button } from "./ui/button";
import { Popover, PopoverPopup, PopoverTrigger } from "./ui/popover";
import { SidebarMenuButton, SidebarMenuItem } from "./ui/sidebar";

export function ScheduledSendsMenu() {
  const items = useAllScheduledSends();
  const { environments } = useEnvironments();
  const cancel = useAtomCommand(scheduledSendEnvironment.cancel, { reportFailure: false });
  const [open, setOpen] = useState(false);
  const [now, setNow] = useState(Date.now);
  const [cancelling, setCancelling] = useState<string | null>(null);
  useEffect(() => {
    if (!open) return;
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, [open]);
  const pendingCount = items.filter((item) => item.task.phase === "pending").length;
  if (
    !environments.some(
      (environment) => environment.serverConfig?.environment.capabilities.scheduledSends,
    )
  )
    return null;
  return (
    <SidebarMenuItem className="shrink-0">
      <Popover
        open={open}
        onOpenChange={(value) => {
          setOpen(value);
          if (value) setNow(Date.now());
        }}
      >
        <PopoverTrigger
          render={
            <SidebarMenuButton
              size="icon"
              aria-label={`Scheduled sends (${pendingCount} pending)`}
            />
          }
        >
          <span className="relative inline-flex">
            <Clock3Icon className="size-4" />
            {pendingCount > 0 ? (
              <span className="absolute -top-2 -right-2 rounded-full bg-primary px-1 text-3xs leading-3 text-primary-foreground">
                {pendingCount}
              </span>
            ) : null}
          </span>
        </PopoverTrigger>
        <PopoverPopup side="top" align="start" width="md" padding="compact">
          <div className="flex max-h-96 flex-col gap-2 overflow-y-auto">
            <p className="text-sm font-medium">Scheduled sends</p>
            {items.length === 0 ? (
              <p className="py-3 text-xs text-muted-foreground">No scheduled sends.</p>
            ) : null}
            {items.map(({ environmentId, environmentLabel, task }) => {
              const connected =
                environments.find((environment) => environment.environmentId === environmentId)
                  ?.connection.phase === "connected";
              const key = `${environmentId}:${task.id}`;
              return (
                <div key={key} className="flex items-start gap-2 border-t border-border/50 pt-2">
                  <div className="min-w-0 flex-1">
                    <Link
                      className="block truncate text-sm hover:underline"
                      to="/$environmentId/$threadId"
                      params={buildThreadRouteParams({ environmentId, threadId: task.threadId })}
                      onClick={() => setOpen(false)}
                    >
                      {task.threadTitle}
                    </Link>
                    <p className="line-clamp-2 text-xs text-muted-foreground">{task.preview}</p>
                    <p className="mt-1 text-xs">
                      {task.phase === "pending"
                        ? scheduledSendCountdown(task.scheduledAt, now)
                        : task.phase === "dispatching"
                          ? "Sending…"
                          : task.phase === "attempted"
                            ? "Attempted"
                            : task.phase === "failed"
                              ? "Failed"
                              : task.phase === "skipped"
                                ? "Skipped"
                                : "Expired"}
                      {environments.length > 1 ? ` · ${environmentLabel}` : ""}
                      {!connected ? " · Disconnected" : ""}
                    </p>
                    <time dateTime={task.scheduledAt} className="text-xs text-muted-foreground">
                      {new Date(task.scheduledAt).toLocaleString()}
                    </time>
                    {task.reason ? (
                      <p className="mt-1 text-xs text-muted-foreground">{task.reason}</p>
                    ) : null}
                  </div>
                  {task.phase === "pending" ? (
                    <Button
                      size="icon-xs"
                      variant="ghost"
                      aria-label={`Cancel scheduled send in ${task.threadTitle}`}
                      disabled={!connected || cancelling === key}
                      onClick={async () => {
                        setCancelling(key);
                        try {
                          const result = await cancel({ environmentId, input: { id: task.id } });
                          if (result._tag === "Failure")
                            toastManager.add({
                              type: "error",
                              title: "Could not cancel scheduled send",
                              description: "Reconnect to the server and try again.",
                            });
                          else if (!result.value.cancelled)
                            toastManager.add({
                              type: "info",
                              title: "Scheduled send already attempted or cancelled",
                            });
                        } finally {
                          setCancelling(null);
                        }
                      }}
                    >
                      <XIcon />
                    </Button>
                  ) : null}
                </div>
              );
            })}
          </div>
        </PopoverPopup>
      </Popover>
    </SidebarMenuItem>
  );
}
