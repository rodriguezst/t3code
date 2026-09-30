import { scheduledSendCountdown } from "@t3tools/client-runtime/state/scheduled-sends";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, MessageId } from "@t3tools/contracts";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { Alert, Modal, Pressable, ScrollView, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppText as Text, AppTextInput } from "../../components/AppText";
import { ComposerActionButton } from "../../components/ComposerToolbar";
import { scheduledSendEnvironment, useAllScheduledSends } from "../../state/scheduledSends";
import { useAtomCommand } from "../../state/use-atom-command";
import { environmentServerConfigsAtom } from "../../state/server";

function SchedulePopup({
  title,
  onClose,
  children,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
}) {
  const insets = useSafeAreaInsets();
  return (
    <Modal transparent animationType="fade" onRequestClose={onClose}>
      <View
        className="flex-1 bg-backdrop"
        style={{ paddingTop: insets.top + 48, paddingHorizontal: 16 }}
      >
        <Pressable
          style={{ position: "absolute", inset: 0 }}
          accessibilityLabel="Dismiss scheduled sends"
          onPress={onClose}
        />
        <View className="rounded-2xl bg-sheet-solid p-4" accessibilityViewIsModal>
          <View className="mb-3 flex-row items-center justify-between">
            <Text className="text-base font-t3-bold text-foreground">{title}</Text>
            <ComposerActionButton accessibilityLabel="Close" icon="xmark" onPress={onClose} />
          </View>
          {children}
        </View>
      </View>
    </Modal>
  );
}

export function ScheduledSendsMenu({ environmentId }: { environmentId?: EnvironmentId }) {
  const configs = useAtomValue(environmentServerConfigsAtom);
  const all = useAllScheduledSends();
  const tasks = environmentId ? all.filter((item) => item.environmentId === environmentId) : all;
  const cancel = useAtomCommand(scheduledSendEnvironment.cancel, { reportFailure: false });
  const [open, setOpen] = useState(false);
  const [now, setNow] = useState(Date.now);
  const [cancelling, setCancelling] = useState<string | null>(null);
  useEffect(() => {
    if (!open) return;
    const timer = setInterval(() => setNow(Date.now()), 30_000);
    return () => clearInterval(timer);
  }, [open]);
  const pendingCount = tasks.filter((item) => item.task.phase === "pending").length;
  const supported = environmentId
    ? configs.get(environmentId)?.environment.capabilities.scheduledSends === true
    : Array.from(configs.values()).some(
        (config) => config.environment.capabilities.scheduledSends === true,
      );
  if (!supported) return null;
  return (
    <>
      <View className="relative">
        <ComposerActionButton
          accessibilityLabel={`Scheduled sends (${pendingCount} pending)`}
          icon="timer"
          onPress={() => {
            setNow(Date.now());
            setOpen(true);
          }}
        />
        {pendingCount > 0 ? (
          <View
            pointerEvents="none"
            className="absolute top-0 right-0 rounded-full bg-primary px-1"
          >
            <Text className="text-2xs text-primary-foreground">{pendingCount}</Text>
          </View>
        ) : null}
      </View>
      {open ? (
        <SchedulePopup title="Scheduled sends" onClose={() => setOpen(false)}>
          <ScrollView style={{ maxHeight: 360 }} keyboardShouldPersistTaps="handled">
            {tasks.length === 0 ? (
              <Text className="py-3 text-sm text-foreground-muted">No scheduled sends.</Text>
            ) : null}
            {tasks.map(({ task, environmentId: ownerId, environmentLabel }) => (
              <View
                key={`${ownerId}:${task.id}`}
                className="flex-row items-start gap-2 border-t border-border py-3"
              >
                <View className="flex-1">
                  <Text className="text-sm font-t3-semibold text-foreground">
                    {task.threadTitle}
                  </Text>
                  {!environmentId ? (
                    <Text className="text-xs text-foreground-muted">{environmentLabel}</Text>
                  ) : null}
                  <Text numberOfLines={2} className="text-xs text-foreground-muted">
                    {task.preview}
                  </Text>
                  <Text className="mt-1 text-xs text-foreground">
                    {task.phase === "pending"
                      ? scheduledSendCountdown(task.scheduledAt, now)
                      : task.phase === "dispatching"
                        ? "Sending…"
                        : task.phase === "attempted"
                          ? "Attempted"
                          : task.phase === "skipped"
                            ? "Skipped"
                            : task.phase === "failed"
                              ? "Failed"
                              : "Expired"}
                  </Text>
                  {task.reason ? (
                    <Text className="mt-1 text-xs text-foreground-muted">{task.reason}</Text>
                  ) : null}
                </View>
                {task.phase === "pending" ? (
                  <ComposerActionButton
                    icon="xmark"
                    accessibilityLabel={`Cancel scheduled send in ${task.threadTitle}`}
                    disabled={cancelling === task.id}
                    onPress={async () => {
                      setCancelling(task.id);
                      try {
                        const result = await cancel({
                          environmentId: ownerId,
                          input: { id: task.id },
                        });
                        if (result._tag === "Failure")
                          Alert.alert("Could not cancel", "Reconnect to the server and try again.");
                        else if (!result.value.cancelled)
                          Alert.alert("Already attempted or cancelled");
                      } finally {
                        setCancelling(null);
                      }
                    }}
                  />
                ) : null}
              </View>
            ))}
          </ScrollView>
        </SchedulePopup>
      ) : null}
    </>
  );
}

export function ScheduleSendControl({
  disabled,
  onSchedule,
}: {
  disabled: boolean;
  onSchedule: (scheduledAt: string) => Promise<MessageId | null>;
}) {
  const [open, setOpen] = useState(false);
  const [hours, setHours] = useState("3");
  const [submitting, setSubmitting] = useState(false);
  const targetRef = useRef<{ hours: string; scheduledAt: string } | null>(null);
  const delay = Number(hours);
  const valid = Number.isFinite(delay) && delay > 0 && delay * 3_600_000 < 8e15;
  return (
    <>
      <ComposerActionButton
        accessibilityLabel="Schedule send"
        icon="clock"
        disabled={disabled || submitting}
        onPress={() => setOpen(true)}
      />
      {open ? (
        <SchedulePopup
          title="Schedule send"
          onClose={() => {
            if (!submitting) setOpen(false);
          }}
        >
          <Text className="mb-2 text-sm text-foreground">Send in (hours)</Text>
          <AppTextInput
            keyboardType="decimal-pad"
            value={hours}
            onChangeText={setHours}
            editable={!submitting}
            accessibilityLabel="Send in hours"
            className="mb-3 rounded-lg bg-subtle p-3 text-base text-foreground"
          />
          <Text className="mb-3 text-xs text-foreground-muted">
            Attempts once while this server stays running. Stops on server restart. If the thread is
            busy or the deadline is missed, it skips the send.
          </Text>
          <Pressable
            accessibilityRole="button"
            disabled={disabled || !valid || submitting}
            className="items-center rounded-lg bg-primary p-3 disabled:opacity-50"
            onPress={async () => {
              if (!valid || submitting) return;
              setSubmitting(true);
              try {
                if (targetRef.current?.hours !== hours)
                  targetRef.current = {
                    hours,
                    scheduledAt: new Date(Date.now() + delay * 3_600_000).toISOString(),
                  };
                if ((await onSchedule(targetRef.current.scheduledAt)) !== null) {
                  setOpen(false);
                  targetRef.current = null;
                }
              } finally {
                setSubmitting(false);
              }
            }}
          >
            <Text className="text-sm font-t3-semibold text-primary-foreground">
              {submitting ? "Scheduling…" : "Schedule send"}
            </Text>
          </Pressable>
        </SchedulePopup>
      ) : null}
    </>
  );
}
