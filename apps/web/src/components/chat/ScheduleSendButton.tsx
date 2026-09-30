import { Clock3Icon } from "lucide-react";
import { useRef, useState } from "react";

import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { composerFloatingLayerProps } from "./composerEventScope";

export function ScheduleSendButton({
  disabled,
  onSchedule,
}: {
  disabled: boolean;
  onSchedule: (scheduledAt: string) => Promise<boolean>;
}) {
  const [open, setOpen] = useState(false);
  const [hours, setHours] = useState("3");
  const [submitting, setSubmitting] = useState(false);
  const targetRef = useRef<{ hours: string; scheduledAt: string } | null>(null);
  const delay = Number(hours);
  const valid = Number.isFinite(delay) && delay > 0 && delay * 3_600_000 < 8e15;
  const submit = async () => {
    if (!valid || submitting) return;
    setSubmitting(true);
    try {
      if (targetRef.current?.hours !== hours)
        targetRef.current = {
          hours,
          scheduledAt: new Date(Date.now() + delay * 3_600_000).toISOString(),
        };
      if (await onSchedule(targetRef.current.scheduledAt)) {
        setOpen(false);
        targetRef.current = null;
      }
    } finally {
      setSubmitting(false);
    }
  };
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={
          <Button variant="ghost" size="icon-sm" disabled={disabled} aria-label="Schedule send" />
        }
      >
        <Clock3Icon />
      </PopoverTrigger>
      <PopoverPopup side="top" align="end" width="sm" {...composerFloatingLayerProps}>
        <div className="flex flex-col gap-3">
          <label className="flex flex-col gap-1.5 text-sm">
            Send in (hours)
            <Input
              type="number"
              min="0.01"
              step="any"
              value={hours}
              onChange={(event) => setHours(event.target.value)}
              disabled={submitting}
            />
          </label>
          <p className="text-xs text-muted-foreground">
            Attempts once while this server stays running. Stops on server restart. If the thread is
            busy or the deadline is missed, it skips the send.
          </p>
          <Button disabled={!valid || disabled || submitting} onClick={() => void submit()}>
            {submitting ? "Scheduling…" : "Schedule send"}
          </Button>
        </div>
      </PopoverPopup>
    </Popover>
  );
}
