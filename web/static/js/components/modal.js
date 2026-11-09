export function confirmModal(title, message, confirmLabel = "Xác nhận") {
  return new Promise((resolve) => {
    const previousFocus = document.activeElement;
    const scrim = document.createElement("div");
    scrim.className = "modal-scrim";
    const dialog = document.createElement("div");
    dialog.className = "modal panel";
    dialog.setAttribute("role", "dialog");
    dialog.setAttribute("aria-modal", "true");
    const heading = document.createElement("h2");
    heading.textContent = title;
    const copy = document.createElement("p");
    copy.textContent = message;
    const cancel = document.createElement("button");
    cancel.className = "button-secondary";
    cancel.textContent = "Huỷ";
    const confirm = document.createElement("button");
    confirm.className = "button-danger";
    confirm.textContent = confirmLabel;
    const actions = document.createElement("div");
    actions.className = "modal-actions";
    actions.append(cancel, confirm);
    dialog.append(heading, copy, actions);
    scrim.append(dialog);
    document.body.append(scrim);
    const close = (value) => {
      scrim.remove();
      previousFocus?.focus?.();
      document.removeEventListener("keydown", onKeyDown);
      resolve(value);
    };
    const onKeyDown = (event) => {
      if (event.key === "Escape") { event.preventDefault(); close(false); }
      if (event.key !== "Tab") return;
      const focusable = [...dialog.querySelectorAll("button:not(:disabled), [href], input:not(:disabled), [tabindex]:not([tabindex='-1'])")];
      if (focusable.length < 2) return;
      if (event.shiftKey && document.activeElement === focusable[0]) { event.preventDefault(); focusable.at(-1).focus(); }
      else if (!event.shiftKey && document.activeElement === focusable.at(-1)) { event.preventDefault(); focusable[0].focus(); }
    };
    cancel.addEventListener("click", () => close(false));
    confirm.addEventListener("click", () => close(true));
    scrim.addEventListener("click", (event) => { if (event.target === scrim) close(false); });
    document.addEventListener("keydown", onKeyDown);
    confirm.focus();
  });
}
