(() => {
  const initialiseFlow = (root = document) => {
    const flow = root.querySelector(".pcm-flow");
    if (!flow || flow.dataset.initialised === "true") return;

    const tabs = [...flow.querySelectorAll('[role="tab"]')];
    const panels = [...flow.querySelectorAll('[role="tabpanel"]')];

    const activate = (tab, moveFocus = false) => {
      const step = tab.dataset.flowStep;
      flow.dataset.activeStep = step;

      tabs.forEach((item) => {
        const active = item === tab;
        item.classList.toggle("is-active", active);
        item.setAttribute("aria-selected", String(active));
        item.tabIndex = active ? 0 : -1;
      });

      panels.forEach((panel) => {
        const active = panel.dataset.flowPanel === step;
        panel.hidden = !active;
        panel.classList.toggle("is-active", active);
      });

      if (moveFocus) tab.focus();
    };

    tabs.forEach((tab, index) => {
      tab.addEventListener("click", () => activate(tab));
      tab.addEventListener("keydown", (event) => {
        let nextIndex;
        if (event.key === "ArrowRight" || event.key === "ArrowDown") nextIndex = (index + 1) % tabs.length;
        if (event.key === "ArrowLeft" || event.key === "ArrowUp") nextIndex = (index - 1 + tabs.length) % tabs.length;
        if (event.key === "Home") nextIndex = 0;
        if (event.key === "End") nextIndex = tabs.length - 1;
        if (nextIndex === undefined) return;
        event.preventDefault();
        activate(tabs[nextIndex], true);
      });
    });

    const selected = tabs.find((tab) => tab.getAttribute("aria-selected") === "true") || tabs[0];
    activate(selected);
    flow.dataset.initialised = "true";
  };

  if (typeof document$ !== "undefined") document$.subscribe(() => initialiseFlow());
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => initialiseFlow(), { once: true });
  } else {
    initialiseFlow();
  }
})();
