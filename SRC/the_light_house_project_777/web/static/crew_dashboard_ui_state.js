"use strict";

(function crewDashboardUiStateBootstrap() {
  const STORAGE_KEY = "crew_dashboard_ui_state_v1";

  function safeParse(raw) {
    try {
      return JSON.parse(String(raw || ""));
    } catch (_error) {
      return null;
    }
  }

  function loadSnapshot() {
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      const parsed = safeParse(raw);
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch (_error) {
      return null;
    }
  }

  function saveSnapshot(payload) {
    try {
      const snapshot = {
        version: 1,
        saved_at: new Date().toISOString(),
        ...(payload || {}),
      };
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(snapshot));
      return snapshot;
    } catch (_error) {
      return null;
    }
  }

  function clearSnapshot() {
    try {
      window.localStorage.removeItem(STORAGE_KEY);
    } catch (_error) {
      // ignore
    }
  }

  function confirmRestore(snapshot) {
    const row = snapshot || {};
    const savedAt = String(row.saved_at || "").trim() || "unknown time";
    const selectedModule = String(row.selectedModule || "none").trim() || "none";
    const moduleState = row.modules && typeof row.modules === "object" ? row.modules : {};
    const activeModules = Object.keys(moduleState).filter((key) => Boolean(moduleState[key]));
    const filter = String(row.logFilter || "all").trim() || "all";
    const autoGeneration = Boolean(row.generationEnabled) ? "ON" : "OFF";
    const message =
      "[ALERT] Saved dashboard button state was found.\n\n" +
      `Saved at: ${savedAt}\n` +
      `Selected module: ${selectedModule}\n` +
      `Active modules: ${activeModules.length ? activeModules.join(", ") : "none"}\n` +
      `Log filter: ${filter}\n` +
      `Auto generation: ${autoGeneration}\n\n` +
      "Press OK to restore the saved button state.\n" +
      "Press Cancel to clear it and start from the default deactivated state.";
    return window.confirm(message);
  }

  window.CrewDashboardUiState = {
    loadSnapshot,
    saveSnapshot,
    clearSnapshot,
    confirmRestore,
  };
})();
