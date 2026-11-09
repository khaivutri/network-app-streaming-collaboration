export function connectEvents(onEvent, onError, onReconnect = () => {}) {
  const source = new EventSource("/api/v1/events");
  let hasOpened = false;
  let wasInterrupted = false;
  const names = [
    "presence.updated",
    "chat.message_received",
    "chat.delivery_status",
    "file.progress",
    "file.completed",
    "file.failed",
    "connection.status",
  ];
  for (const name of names) {
    source.addEventListener(name, (event) => {
      try { onEvent(name, JSON.parse(event.data)); } catch (error) { onError(error); }
    });
  }
  source.onopen = () => {
    if (hasOpened && wasInterrupted) onReconnect();
    hasOpened = true;
    wasInterrupted = false;
  };
  source.onerror = (error) => {
    wasInterrupted = true;
    onError(error);
  };
  return () => source.close();
}
