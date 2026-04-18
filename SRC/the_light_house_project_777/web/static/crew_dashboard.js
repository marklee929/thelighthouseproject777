"use strict";

(function crewDashboardBootstrap() {
  const root = document.getElementById("crew-root");
  if (!root) return;

  const MONITOR_INTERVAL_MS = 60 * 1000;
  const DEFAULT_GENERATION_INTERVAL_MS = 10 * 60 * 1000;
  const MAX_LOG_LINES = 400;
  const QUICK_ACTION = {
    "New Task": "/task new",
    "Generate Thread": "/social generate facebook posts",
    "Generate Clips": "/content generate workconnect clips",
    "Research Topic": "/social collect news foreign workers korea",
    Publish: "/social publish facebook",
  };

  const dom = {
    moduleButtons: Array.from(document.querySelectorAll(".crew-module-item[data-module]")),
    quickActionButtons: Array.from(document.querySelectorAll(".crew-quick-action-btn[data-action]")),
    globalLogBox: document.getElementById("global-log-box"),
    filterButtons: Array.from(document.querySelectorAll(".log-filter-btn[data-filter]")),
    commandForm: document.getElementById("crew-command-form"),
    commandInput: document.getElementById("crew-command-input"),
    commandResponse: document.getElementById("crew-command-response"),
    activityLines: document.getElementById("activity-lines"),
    taskQueueList: document.getElementById("task-queue-list"),
    taskQueueSummary: document.getElementById("task-queue-summary"),
    taskQueueEmpty: document.getElementById("task-queue-empty"),
    moduleConfigContent: document.getElementById("selected-module-config-content"),
    publishList: document.getElementById("publish-review-list"),
    publishCount: document.getElementById("publish-review-count"),
    publishEmpty: document.getElementById("publish-review-empty"),
    newsCollectorScreen: document.getElementById("news-collector-screen"),
    newsCollectorList: document.getElementById("news-collector-list"),
    newsCollectorCount: document.getElementById("news-collector-count"),
    newsCollectorEmpty: document.getElementById("news-collector-empty"),
    modal: document.getElementById("draft-preview-modal"),
    modalCategory: document.getElementById("draft-preview-category-input"),
    modalSourceLink: document.getElementById("draft-preview-source-link-input"),
    modalArticleTitle: document.getElementById("draft-preview-article-title-input"),
    modalThumbnailWrap: document.getElementById("draft-preview-thumbnail-wrap"),
    modalThumbnail: document.getElementById("draft-preview-thumbnail"),
    modalTitle: document.getElementById("draft-preview-title-input"),
    modalBody: document.getElementById("draft-preview-body-input"),
    modalEdit: document.getElementById("draft-preview-edit-btn"),
    modalContinue: document.getElementById("draft-preview-continue-btn"),
    modalStop: document.getElementById("draft-preview-stop-btn"),
  };

  const moduleOrder = dom.moduleButtons.map((button) => button.dataset.module).filter(Boolean);
  const uiStateBridge = window.CrewDashboardUiState || {
    loadSnapshot: () => null,
    saveSnapshot: () => null,
    clearSnapshot: () => {},
    confirmRestore: () => false,
  };
  const modules = {};
  moduleOrder.forEach((name) => {
    modules[name] = { active: false, status: "idle" };
  });

  const appState = {
    selectedModule: null,
    modules,
    logFilter: "all",
    generationEnabled: false,
    generationState: "idle",
    platformProfiles: {
      facebook: {
        format: "post",
        tone: "practical",
        length: "medium",
      },
      x: {
        format: "thread",
        tone: "hook",
        length: "short",
      },
    },
    platformConfig: {
      facebook: {
        format: "post",
        tone: "practical",
        length: "medium",
      },
    },
    accounts: {
      default_social: { facebook_page: "main_page", x_account: "experimental" },
    },
    facebookAuthConfig: {
      appId: "",
      appSecret: "",
      pageId: "",
      userLongLivedAccessToken: "",
      shortUserAccessToken: "",
    },
    content: {
      selectedMode: "workconnect_clips",
      modes: ["workconnect_clips"],
      queue: [],
      quality: {
        approved: 0,
        rejected: 0,
        category_stats: {},
        variant_stats: {},
      },
      generationInFlight: false,
    },
    socialForm: {
      platform: "facebook",
      targetPage: "main_page",
      newsSource: "naver",
      keywordSet: "foreign_workers_korea",
      mode: "review_before_publish",
      topic: "",
      tone: "analytical",
      length: "short",
      options: {
        includeHashtags: false,
        includeCTA: false,
        includeLinks: false,
      },
    },
    xAuth: {
      connected: false,
      status: "facebook_config_missing",
      message: "Missing Facebook Page credentials.",
    },
    currentPostId: null,
    currentPostCreatedAt: null,
    postMonitor: {
      monitoring: false,
      closed: false,
      last_like_count: 0,
      last_fetch_count: 0,
      last_fetch_at: null,
    },
    publishQueue: [],
    publishCardCollapse: {},
    lastDraftIssueSignature: "",
    preview: {
      open: false,
      editing: false,
      draft: null,
      context: null,
    },
    growth: {
      postId: "",
      pendingApprovals: [],
    },
    localLikerCache: {},
    oauth: {
      pendingAutoEnable: false,
    },
    loop: {
      running: false,
      timer: null,
      tickInFlight: false,
      cycleInFlight: false,
      nextCycleAt: null,
      lastGenerationAt: null,
      generationIntervalMs: DEFAULT_GENERATION_INTERVAL_MS,
      lastNoPostLogAt: 0,
      lastBootstrapDeferredLogAt: 0,
      lastGateLogAt: 0,
      currentCycleId: null,
    },
  };
  const newsCollectorController =
    window.CrewNewsCollector && typeof window.CrewNewsCollector.createController === "function"
      ? window.CrewNewsCollector.createController({
          root,
          appState,
          dom,
          helpers: {
            apiJson,
            appendLog,
            appendApiError,
            esc,
            setActivity,
            rerender,
          },
        })
      : null;

  function captureUiButtonState() {
    const moduleFlags = {};
    Object.keys(appState.modules || {}).forEach((name) => {
      moduleFlags[name] = Boolean(appState.modules[name] && appState.modules[name].active);
    });
    return {
      selectedModule: appState.selectedModule || "",
      modules: moduleFlags,
      logFilter: String(appState.logFilter || "all").toLowerCase(),
      generationEnabled: Boolean(appState.generationEnabled),
      contentSelectedMode: String((appState.content && appState.content.selectedMode) || "workconnect_clips"),
    };
  }

  function isDefaultUiButtonState(snapshot) {
    const row = snapshot || {};
    const selectedModule = String(row.selectedModule || "").trim();
    const logFilter = String(row.logFilter || "all").toLowerCase();
    const contentSelectedMode = String(row.contentSelectedMode || "workconnect_clips").trim() || "workconnect_clips";
    const generationEnabled = Boolean(row.generationEnabled);
    const moduleFlags = row.modules && typeof row.modules === "object" ? row.modules : {};
    const hasActiveModule = Object.keys(moduleFlags).some((name) => Boolean(moduleFlags[name]));
    return !selectedModule && !hasActiveModule && !generationEnabled && logFilter === "all" && contentSelectedMode === "workconnect_clips";
  }

  function persistUiButtonState() {
    const snapshot = captureUiButtonState();
    if (isDefaultUiButtonState(snapshot)) {
      uiStateBridge.clearSnapshot();
      return;
    }
    uiStateBridge.saveSnapshot(snapshot);
  }

  function applyDefaultUiButtonState() {
    appState.selectedModule = null;
    Object.keys(appState.modules || {}).forEach((name) => {
      if (!appState.modules[name]) return;
      appState.modules[name].active = false;
      appState.modules[name].status = "idle";
    });
    appState.logFilter = "all";
    appState.generationEnabled = false;
    appState.oauth.pendingAutoEnable = false;
    if (appState.content) {
      appState.content.selectedMode = "workconnect_clips";
    }
  }

  async function maybeRestoreUiButtonState() {
    const snapshot = uiStateBridge.loadSnapshot();
    if (!snapshot) {
      applyDefaultUiButtonState();
      persistUiButtonState();
      rerender();
      return;
    }

    const restore = uiStateBridge.confirmRestore(snapshot);
    if (!restore) {
      uiStateBridge.clearSnapshot();
      applyDefaultUiButtonState();
      stopGenerationLoop();
      persistUiButtonState();
      appendLog("system", "[SYSTEM] Saved dashboard button state discarded. Default deactivated state applied.");
      rerender();
      return;
    }

    const moduleFlags = snapshot.modules && typeof snapshot.modules === "object" ? snapshot.modules : {};
    Object.keys(appState.modules || {}).forEach((name) => {
      if (!appState.modules[name]) return;
      appState.modules[name].active = Boolean(moduleFlags[name]);
    });
    appState.selectedModule = appState.modules[snapshot.selectedModule] ? snapshot.selectedModule : null;
    appState.logFilter = String(snapshot.logFilter || "all").toLowerCase() || "all";
    if (
      appState.content &&
      Array.isArray(appState.content.modes) &&
      appState.content.modes.includes(String(snapshot.contentSelectedMode || ""))
    ) {
      appState.content.selectedMode = String(snapshot.contentSelectedMode || appState.content.selectedMode || "workconnect_clips");
    }
    rerender();
    if (snapshot.generationEnabled) {
      await toggleAutoGeneration(true);
    } else {
      appState.generationEnabled = false;
      renderModuleConfig();
    }
    persistUiButtonState();
    appendLog("system", "[SYSTEM] Saved dashboard button state restored from local JSON snapshot.");
  }

  function esc(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function cap(value) {
    const text = String(value || "");
    if (!text) return "";
    return text.charAt(0).toUpperCase() + text.slice(1);
  }

  function nowTimeLabel() {
    const now = new Date();
    return now.toTimeString().slice(0, 8);
  }

  function flowLabel(state) {
    const map = {
      idle: "idle",
      bootstrap: "bootstrap",
      collecting: "collecting",
      monitoring: "monitoring",
      generating: "generating",
      reviewing: "reviewing",
      telegram_pending: "telegram pending",
      approved_for_publish: "approved for publish",
      rejected: "rejected",
      published: "published",
      waiting_approval: "waiting approval",
      publishing_review: "publishing review",
      complete: "complete",
      stopped: "stopped",
      error: "error",
    };
    return map[state] || state;
  }

  function xAuthLabel(status) {
    const raw = String(status || "").trim();
    const key = raw.toUpperCase();
    const map = {
      READY: "Ready",
      CONFIG_MISSING: "Config Missing",
      TOKEN_EXPIRED: "Token Expired",
      PERMISSION_INVALID: "Permission Invalid",
      PAGE_UNREACHABLE: "Page Unreachable",
      DRY_RUN_ONLY: "Dry Run Only",
      EXPERIMENTAL: "Experimental",
    };
    return map[key] || cap(raw.toLowerCase() || "unknown");
  }

  function xAuthTone(status, connected) {
    if (connected) return "complete";
    const key = String(status || "").trim().toUpperCase();
    if (key.includes("PENDING")) return "monitoring";
    if (key.includes("EXPIRED") || key.includes("INVALID") || key.includes("MISSING") || key.includes("UNREACHABLE")) return "error";
    return "idle";
  }

  function parseTagged(message) {
    const text = String(message || "").trim();
    const matched = text.match(/^\[([A-Z_]+)\]\s*(.*)$/i);
    if (!matched) return { type: "system", text };
    const tag = matched[1].toLowerCase();
    const map = {
      system: "system",
      agent: "agent",
      preview: "preview",
      publish: "publish",
      growth: "growth",
      follow: "follow",
      error: "error",
    };
    return { type: map[tag] || "system", text };
  }

  function appendLog(type, message) {
    if (!dom.globalLogBox) return;
    const line = document.createElement("div");
    line.className = "global-log-line";
    line.dataset.type = String(type || "system");
    line.textContent = `[${nowTimeLabel()}] ${String(message || "")}`;
    dom.globalLogBox.appendChild(line);
    while (dom.globalLogBox.children.length > MAX_LOG_LINES) {
      dom.globalLogBox.removeChild(dom.globalLogBox.firstElementChild);
    }
    applyLogFilter();
    dom.globalLogBox.scrollTop = dom.globalLogBox.scrollHeight;
  }

  function appendTagged(message) {
    const tagged = parseTagged(message);
    appendLog(tagged.type, tagged.text);
  }

  function appendApiError(error) {
    const payloadLogs = error && error.payload && Array.isArray(error.payload.logs) ? error.payload.logs : [];
    payloadLogs.forEach(appendTagged);
    appendLog("error", `[ERROR] ${String((error && error.message) || error)}`);
  }

  function applyLogFilter() {
    if (!dom.globalLogBox) return;
    const filter = String(appState.logFilter || "all").toLowerCase();
    Array.from(dom.globalLogBox.querySelectorAll(".global-log-line")).forEach((line) => {
      const type = String(line.dataset.type || "system").toLowerCase();
      line.classList.toggle("is-hidden", !(filter === "all" || filter === type));
    });
    renderFilterButtons();
  }

  function renderFilterButtons() {
    dom.filterButtons.forEach((button) => {
      const filter = String(button.dataset.filter || "all").toLowerCase();
      button.classList.toggle("is-active", filter === appState.logFilter);
    });
  }

  async function apiJson(url, init) {
    const response = await fetch(url, init);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.ok === false) {
      const reason = payload.error || `request failed (${response.status})`;
      const detail = payload.detail ? ` | ${payload.detail}` : "";
      const error = new Error(String(reason) + String(detail));
      error.payload = payload;
      throw error;
    }
    return payload;
  }

  function setActivity(lines) {
    if (!dom.activityLines) return;
    dom.activityLines.innerHTML = "";
    lines.forEach((text) => {
      const item = document.createElement("li");
      item.textContent = String(text || "");
      dom.activityLines.appendChild(item);
    });
  }

  function taskIndicatorClass(status) {
    if (status === "running") return "running-indicator";
    if (status === "completed") return "completed-indicator";
    return "pending-indicator";
  }

  function taskCardClass(status) {
    if (status === "running") return "task-queue-item is-running";
    if (status === "completed") return "task-queue-item is-completed";
    return "task-queue-item is-waiting";
  }

  function taskLabelClass(status) {
    if (status === "running") return "task-state-label is-running";
    if (status === "completed") return "task-state-label is-completed";
    return "task-state-label is-waiting";
  }

  function buildNewsCollectorTasks() {
    const newsCollector = appState.newsCollector || {};
    const feeds = Array.isArray(newsCollector.feeds) ? newsCollector.feeds : [];
    const candidates = Array.isArray(newsCollector.candidates) ? newsCollector.candidates : [];
    const connectedCount = Number(newsCollector.connectedCount || 0);
    const scoredCount = candidates.filter((item) => item && item.final_score !== null && item.final_score !== undefined).length;
    const collecting = Boolean(newsCollector.collecting);
    const loadingFeeds = Boolean(newsCollector.loadingFeeds);
    const loadingCandidates = Boolean(newsCollector.loading);
    const tasks = [];

    tasks.push({
      label: "Load Christian RSS Registry",
      status: loadingFeeds ? "running" : feeds.length ? "completed" : "waiting",
      description: loadingFeeds
        ? "Seeding the christian_news_collection registry into PostgreSQL."
        : feeds.length
        ? `${feeds.length} curated RSS feeds are registered.`
        : "Registry seed prepares the initial Christian RSS list.",
    });

    if (feeds.length || loadingFeeds) {
      tasks.push({
        label: "Connect RSS Feeds",
        status: connectedCount > 0 ? "completed" : "waiting",
        description:
          connectedCount > 0
            ? `${connectedCount} feed(s) connected and ready for latest-news collection.`
            : "Select a feed and connect it before collection starts.",
      });
    }

    if (collecting || connectedCount > 0 || candidates.length > 0) {
      tasks.push({
        label: "Collect Latest Christian News",
        status: collecting ? "running" : candidates.length > 0 ? "completed" : "waiting",
        description: collecting
          ? "Fetching only the last-hour RSS articles and storing raw content and metadata."
          : candidates.length > 0
          ? `${candidates.length} article candidate(s) survived the fresh-review gate.`
          : "Start collection to fetch only the last-hour RSS stories.",
      });
      tasks.push({
        label: "Score PLD-Safe Candidates",
        status: collecting || loadingCandidates ? "running" : scoredCount > 0 ? "completed" : "waiting",
        description:
          collecting || loadingCandidates
            ? "Applying PLD-fit, reaction, operational safety, and freshness ranking."
            : scoredCount > 0
            ? `${scoredCount} candidate(s) already have article scores.`
            : "Scoring begins immediately after collection and keeps one article per 10-minute bucket.",
      });
    }

    if (candidates.length > 0 || loadingCandidates) {
      tasks.push({
        label: "Operator Review Cards",
        status: loadingCandidates ? "running" : candidates.length > 0 ? "running" : "waiting",
        description:
          candidates.length > 0
            ? "Approve, modify, reject, or drop cards before they move to Social."
            : "Waiting for reviewable article cards.",
      });
    }

    return tasks.filter((task, index) => task.status !== "waiting" || index < 3 || collecting || candidates.length > 0);
  }

  function buildSocialTasks() {
    const state = String(appState.generationState || "idle");
    const hasDrafts = getActiveDraftCards().length > 0;
    const hasCurrentPost = Boolean(appState.currentPostId);
    const connected = Boolean(appState.xAuth && appState.xAuth.connected);
    const tasks = [];

    tasks.push({
      label: "Connect Facebook Page",
      status: connected ? "completed" : "waiting",
      description: connected ? "Publishing connector is ready." : "Facebook runtime auth must be valid first.",
    });

    if (appState.generationEnabled || state !== "idle" || hasDrafts) {
      tasks.push({
        label: "Collect News",
        status:
          state === "collecting"
            ? "running"
            : hasDrafts || ["generating", "reviewing", "telegram_pending", "approved_for_publish", "published"].includes(state)
            ? "completed"
            : "waiting",
        description:
          state === "collecting"
            ? "Collecting source articles for the current Social cycle."
            : "Collection is complete for the current draft cycle.",
      });
      tasks.push({
        label: "Generate Candidate Facebook Post",
        status:
          state === "generating"
            ? "running"
            : hasDrafts || ["reviewing", "telegram_pending", "approved_for_publish", "published"].includes(state)
            ? "completed"
            : "waiting",
        description:
          state === "generating"
            ? "Generating draft copy and candidate assets."
            : hasDrafts
            ? "Draft generation completed for the current cycle."
            : "Draft generation follows article collection.",
      });
      tasks.push({
        label: "Telegram Approval Review",
        status: ["reviewing", "telegram_pending", "waiting_approval", "publishing_review"].includes(state)
          ? "running"
          : ["approved_for_publish", "published"].includes(state)
          ? "completed"
          : "waiting",
        description:
          ["reviewing", "telegram_pending", "waiting_approval", "publishing_review"].includes(state)
            ? "Waiting for operator review before publish."
            : "Telegram approval is the manual gate before publish.",
      });
      tasks.push({
        label: "Publish to Facebook",
        status: state === "published" ? "completed" : state === "approved_for_publish" ? "running" : "waiting",
        description:
          state === "published" ? "Latest approved draft is already published." : "Publishing starts after approval.",
      });
    } else if (hasCurrentPost) {
      tasks.push({
        label: "Monitor Current Post",
        status: "running",
        description: "Monitoring the current post before the next generation window.",
      });
    }

    return tasks.filter((task, index) => task.status !== "waiting" || index === 0);
  }

  function buildGenericModuleTasks(moduleName) {
    return [
      {
        label: `${moduleName} Execution Flow`,
        status: appState.modules[moduleName] && appState.modules[moduleName].active ? "running" : "waiting",
        description:
          appState.modules[moduleName] && appState.modules[moduleName].active
            ? "This module is selected, but its live execution queue is not modeled yet."
            : "Select and activate this module when its execution flow is ready.",
      },
    ];
  }

  function collectTaskGroups() {
    const groups = [];
    const newsCollector = appState.newsCollector || {};
    const showNewsCollector =
      Boolean(newsCollector.loadingFeeds) ||
      Boolean(newsCollector.loading) ||
      Boolean(newsCollector.collecting) ||
      (Array.isArray(newsCollector.feeds) && newsCollector.feeds.length > 0) ||
      (Array.isArray(newsCollector.candidates) && newsCollector.candidates.length > 0) ||
      Boolean(appState.modules["News Collector"] && appState.modules["News Collector"].active) ||
      appState.selectedModule === "News Collector";
    if (showNewsCollector) {
      groups.push({
        moduleName: "News Collector",
        moduleCode: "NC",
        tasks: buildNewsCollectorTasks(),
      });
    }

    const socialHasWork =
      appState.generationEnabled ||
      String(appState.generationState || "idle") !== "idle" ||
      getActiveDraftCards().length > 0 ||
      Boolean(appState.currentPostId) ||
      Boolean(appState.modules.Social && appState.modules.Social.active) ||
      appState.selectedModule === "Social";
    if (socialHasWork) {
      groups.push({
        moduleName: "Social",
        moduleCode: "SO",
        tasks: buildSocialTasks(),
      });
    }

    if (!groups.length && appState.selectedModule) {
      groups.push({
        moduleName: appState.selectedModule,
        moduleCode: String(appState.selectedModule || "").slice(0, 2).toUpperCase(),
        tasks: buildGenericModuleTasks(appState.selectedModule),
      });
    }

    return groups
      .map((group) => ({
        ...group,
        tasks: (group.tasks || []).map((task) => ({ ...task, moduleName: group.moduleName, moduleCode: group.moduleCode })),
      }))
      .filter((group) => group.tasks.length > 0);
  }

  function renderTaskQueue() {
    if (!dom.taskQueueList || !dom.taskQueueSummary || !dom.taskQueueEmpty) return;
    const groups = collectTaskGroups();
    const visibleTasks = groups.flatMap((group) => group.tasks || []);
    if (!visibleTasks.length) {
      dom.taskQueueList.innerHTML = "";
      dom.taskQueueSummary.textContent = "No Active Tasks";
      dom.taskQueueEmpty.textContent = "Select or run a module to see the live execution flow.";
      dom.taskQueueEmpty.classList.remove("is-hidden");
      return;
    }
    const runningCount = visibleTasks.filter((task) => task.status === "running").length;
    const completedCount = visibleTasks.filter((task) => task.status === "completed").length;
    const waitingCount = visibleTasks.filter((task) => task.status === "waiting").length;
    const moduleSummary = groups.map((group) => group.moduleName).join(" + ");
    dom.taskQueueSummary.textContent = `${moduleSummary} | ${visibleTasks.length} Tasks | ${runningCount} Running${
      completedCount ? ` | ${completedCount} Ready` : ""
    }${waitingCount ? ` | ${waitingCount} Waiting` : ""}`;
    dom.taskQueueEmpty.classList.toggle("is-hidden", visibleTasks.length > 0);

    dom.taskQueueList.innerHTML = visibleTasks
      .map(
        (task) => `<li class="${taskCardClass(task.status)}">
  <span class="task-indicator ${taskIndicatorClass(task.status)}"></span>
  <span class="task-main">
    <span class="task-name"><span class="task-module-chip">${esc(task.moduleCode || "")}</span>${esc(task.label)}</span>
    <span class="task-description">${esc(task.description || "")}</span>
  </span>
  <span class="${taskLabelClass(task.status)}">${esc(task.status)}</span>
</li>`
      )
      .join("");
  }

  function getModuleStatusClass(status) {
    if (status === "running") return "module-status-running";
    if (status === "complete") return "module-status-complete";
    if (status === "error") return "module-status-error";
    return "module-status-idle";
  }

  function getLedClass(status, active) {
    if (status === "running") return "led-running";
    if (status === "complete") return "led-complete";
    if (status === "error") return "led-error";
    if (active) return "led-active";
    return "led-idle";
  }

  function renderModules() {
    dom.moduleButtons.forEach((button) => {
      const moduleName = button.dataset.module;
      const moduleState = appState.modules[moduleName];
      if (!moduleState) return;
      const isSelected = appState.selectedModule === moduleName;
      button.classList.toggle("is-selected", isSelected);
      button.classList.toggle("is-active", Boolean(moduleState.active));
      button.classList.toggle("is-inactive", !moduleState.active);
      button.classList.toggle("is-running", moduleState.status === "running");
      button.setAttribute("aria-pressed", String(Boolean(moduleState.active)));

      const statusEl = button.querySelector(".module-state-text");
      if (statusEl) {
        statusEl.textContent = moduleState.status;
        statusEl.className = `module-state-text ${getModuleStatusClass(moduleState.status)}`;
      }

      const led = button.querySelector("[data-led]");
      if (led) {
        led.className = `crew-module-led ${getLedClass(moduleState.status, moduleState.active)}`;
      }
    });
  }

  function selectModule(moduleName) {
    if (!appState.modules[moduleName]) return;
    appState.selectedModule = moduleName;
    appendLog("system", `[SYSTEM] Module ${moduleName} selected`);
    renderModuleConfig();
    renderModules();
    renderTaskQueue();
    persistUiButtonState();
    if (newsCollectorController) {
      newsCollectorController.handleModuleSelected(moduleName);
    }
  }

  function setModuleStatus(moduleName, status, forceActive) {
    const moduleState = appState.modules[moduleName];
    if (!moduleState) return;
    moduleState.status = status;
    if (forceActive || status === "running") moduleState.active = true;
    if (status === "idle" && !forceActive && !moduleState.active) moduleState.active = false;
    renderModules();
    renderTaskQueue();
  }

  function toggleModuleActive(moduleName) {
    const moduleState = appState.modules[moduleName];
    if (!moduleState) return;
    if (moduleState.status === "running" && moduleState.active) {
      appendLog("error", `[ERROR] Running module ${moduleName} cannot be deactivated`);
      return;
    }
    moduleState.active = !moduleState.active;
    appendLog("system", `[SYSTEM] Module ${moduleName} ${moduleState.active ? "active" : "inactive"}`);
    renderModules();
    renderTaskQueue();
    persistUiButtonState();
  }

  function updateGenerationState(nextState) {
    if (!nextState) return;
    const changed = appState.generationState !== nextState;
    appState.generationState = nextState;
    if (changed) {
      appendLog("system", `[SYSTEM] Generation state -> ${flowLabel(nextState)}`);
    }

    if (nextState === "error") {
      setModuleStatus("Social", "error", true);
    } else if (
      nextState === "collecting" ||
      nextState === "monitoring" ||
      nextState === "generating" ||
      nextState === "reviewing" ||
      nextState === "telegram_pending" ||
      nextState === "approved_for_publish" ||
      nextState === "waiting_approval" ||
      nextState === "publishing_review"
    ) {
      setModuleStatus("Social", "running", true);
    } else if (nextState === "complete" || nextState === "published") {
      setModuleStatus("Social", "complete", true);
    } else {
      setModuleStatus("Social", "idle", appState.modules.Social ? appState.modules.Social.active : false);
    }

    renderModuleConfig();
    renderTaskQueue();
  }

  function normalizePublishDraft(row) {
    const item = row || {};
    const body = String(item.body || "");
    const preview = String(item.body_preview || item.bodyPreview || (body.length > 220 ? `${body.slice(0, 220)}...` : body));
    return {
      draftId: String(item.draft_id || item.draftId || item.id || ""),
      platform: String(item.platform || "facebook").toUpperCase(),
      category: String(item.category || "general"),
      articleTitle: String(item.article_title || item.articleTitle || ""),
      sourceLink: String(item.source_link || item.sourceLink || ""),
      thumbnailUrl: String(item.thumbnail_url || item.thumbnailUrl || ""),
      title: String(item.title || "Korea Update"),
      body,
      bodyPreview: preview,
      cycleId: String(item.cycle_id || item.cycleId || ""),
      relevanceScore: Number(item.relevance_score || item.relevanceScore || 0),
      researchRelevanceScore: Number(item.research_relevance_score || item.researchRelevanceScore || item.relevance_score || 0),
      whyItMatters: String(item.why_it_matters || item.whyItMatters || ""),
      targetAudience: String(item.target_audience || item.targetAudience || ""),
      postAngle: String(item.post_angle || item.postAngle || ""),
      postSummary: String(item.post_summary || item.postSummary || ""),
      summary: String(item.summary || item.post_summary || item.postSummary || ""),
      generatedPost: String(item.generated_post || item.generatedPost || body),
      riskOfMisleading: String(item.risk_of_misleading || item.riskOfMisleading || "medium"),
      finalRecommendation: String(item.final_recommendation || item.finalRecommendation || "revise"),
      reviewNotes: Array.isArray(item.review_notes) ? item.review_notes : [],
      contentQualityScore: Number(item.content_quality_score || item.contentQualityScore || 0),
      articleRelevant: Boolean(item.article_relevant),
      postTone: String(item.post_tone || item.postTone || "neutral"),
      approvalChannel: String(item.approval_channel || item.approvalChannel || "telegram"),
      approvalStatus: String(item.approval_status || item.approvalStatus || "pending"),
      operatorOverridePublish: Boolean(item.operator_override_publish || item.operatorOverridePublish || false),
      telegramSentAt: String(item.telegram_sent_at || item.telegramSentAt || ""),
      lastTelegramError: String(item.last_telegram_error || item.lastTelegramError || ""),
      operatorDecision: String(item.operator_decision || item.operatorDecision || ""),
      operatorModified: Boolean(item.operator_modified || item.operatorModified || false),
      published: Boolean(item.published || false),
    };
  }

  function normalizeContentClip(row) {
    const item = row || {};
    return {
      clipId: String(item.clip_id || item.clipId || item.id || ""),
      mode: String(item.mode || "workconnect_clips"),
      category: String(item.category || ""),
      format: String(item.format_label || item.format || ""),
      topic: String(item.topic || ""),
      title: String(item.title || "WorkConnect Clip"),
      hook: String(item.hook || ""),
      summary: String(item.summary || ""),
      keyMessage: String(item.key_message || item.keyMessage || ""),
      researchQuality: String(item.research_quality || item.researchQuality || ""),
      sourcesUsedCount: Number(item.sources_used_count || item.sourcesUsedCount || 0),
      visualCoverage: String(item.visual_coverage || item.visualCoverage || ""),
      previewPath: String(item.preview_path || item.previewPath || ""),
      posterPath: String(item.poster_path || item.posterPath || ""),
      manifestPath: String(item.manifest_path || item.manifestPath || ""),
      renderStatus: String(item.render_status || item.renderStatus || ""),
      approvalStatus: String(item.approval_status || item.approvalStatus || "pending"),
      telegramSentAt: String(item.telegram_sent_at || item.telegramSentAt || ""),
      lastTelegramError: String(item.last_telegram_error || item.lastTelegramError || ""),
      videoPath: String(item.video_path || item.videoPath || ""),
    };
  }

  async function resolveContentClip(clipId, decision) {
    const cid = String(clipId || "").trim();
    const action = String(decision || "").trim().toLowerCase();
    if (!cid || !action) return;
    try {
      const response = await apiJson("/api/crew/content/review", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ clip_id: cid, decision: action }),
      });
      (response.logs || []).forEach(appendTagged);
      appState.content.queue = response.content_queue || [];
      appState.content.quality = response.content_quality || appState.content.quality;
      renderModuleConfig();
      appendLog("system", `[SYSTEM] Content clip ${cid} ${action === "approve" ? "approved" : "rejected"} and archived`);
    } catch (error) {
      appendApiError(error);
    }
  }

  function renderContentQueueList() {
    const rows = (appState.content.queue || []).map(normalizeContentClip);
    if (!rows.length) {
      return '<div class="module-config-empty">No pending clip reviews.</div>';
    }
    return rows
      .map(
        (row) => `<article class="approval-item">
  <div class="approval-item-header">
    <span class="approval-username">${esc(row.title)}</span>
    <span class="approval-meta">${esc(row.approvalStatus)} / ${esc(row.format || "-")}</span>
  </div>
  <div class="approval-meta">Category: ${esc(row.category)} | Topic: ${esc(row.topic)}</div>
  <div class="approval-meta">Hook: ${esc(row.hook || "-")}</div>
  <div class="approval-meta">Key message: ${esc(row.keyMessage || "-")}</div>
  <div class="approval-meta">Research: ${esc(row.researchQuality || "-")} | Sources: ${esc(row.sourcesUsedCount || 0)} | Visual Coverage: ${esc(row.visualCoverage || "-")}</div>
  <div class="approval-meta">Telegram sent: ${esc(row.telegramSentAt || "-")}</div>
  <div class="approval-meta">Video: ${row.videoPath ? `<a class="publish-source-link" href="${esc(row.videoPath)}" target="_blank" rel="noreferrer">open</a>` : "-"}</div>
  <div class="approval-meta">Preview: ${row.previewPath ? `<a class="publish-source-link" href="${esc(row.previewPath)}" target="_blank" rel="noreferrer">open</a>` : "-"}</div>
  <div class="approval-meta">Manifest: ${row.manifestPath ? `<a class="publish-source-link" href="${esc(row.manifestPath)}" target="_blank" rel="noreferrer">open</a>` : "-"}</div>
  <div class="approval-meta">Telegram error: ${esc(row.lastTelegramError || "-")}</div>
  <div class="approval-actions">
    <button type="button" class="crew-btn primary" data-content-action="approve" data-clip-id="${esc(row.clipId)}">Approve</button>
    <button type="button" class="crew-btn danger" data-content-action="reject" data-clip-id="${esc(row.clipId)}">Reject</button>
  </div>
</article>`
      )
      .join("");
  }

  function renderPublishCards() {
    if (!dom.publishList || !dom.publishCount || !dom.publishEmpty) return;
    const cards = (appState.publishQueue || []).map(normalizePublishDraft);
    dom.publishCount.textContent = `${cards.length} Items`;
    dom.publishEmpty.classList.toggle("is-hidden", cards.length > 0);

    if (!cards.length) {
      dom.publishList.innerHTML = "";
      return;
    }

    dom.publishList.innerHTML = cards
      .map(
        (card) => {
          const collapsed = appState.publishCardCollapse[card.draftId] !== false;
          const canApprovePublish = card.approvalStatus === "approved" && !card.published;
          return `<article class="publish-card ${collapsed ? "is-collapsed" : ""}" data-card-id="${esc(card.draftId)}">
  <header class="publish-card-header">
    <div class="publish-card-header-main">
      <span class="publish-platform"><span class="platform-icon">F</span>Platform: ${esc(card.platform)}</span>
      <div class="publish-card-header-summary">
        <strong>${esc(card.title)}</strong>
        <span>${esc(card.approvalStatus || "pending")} / ${esc(card.finalRecommendation || "revise")}</span>
      </div>
    </div>
    <button type="button" class="crew-btn ghost publish-card-toggle" data-publish-toggle="card" data-card-id="${esc(card.draftId)}">
      ${collapsed ? "Expand" : "Collapse"}
    </button>
  </header>
  <div class="publish-card-content">
  <p class="approval-meta">Category: ${esc(card.category)}</p>
  <p class="approval-meta">Research Relevance Score: ${esc(card.researchRelevanceScore || 0)}</p>
  <p class="approval-meta">Final Recommendation: ${esc(card.finalRecommendation || "revise")}</p>
  <p class="approval-meta">Approval Status: ${esc(card.approvalStatus || "pending")} via ${esc(card.approvalChannel || "telegram")}</p>
  <p class="approval-meta">Risk of Misleading: ${esc(card.riskOfMisleading || "medium")}</p>
  <p class="approval-meta">Telegram Sent: ${esc(card.telegramSentAt || "-")}</p>
  <p class="approval-meta">Telegram Error: ${esc(card.lastTelegramError || "-")}</p>
  <p class="approval-meta">Operator Decision: ${esc(card.operatorDecision || "-")}${card.operatorModified ? " (modified)" : ""}</p>
  <p class="approval-meta">Article: ${esc(card.articleTitle)}</p>
  ${
    card.thumbnailUrl
      ? `<div class="publish-thumb-wrap"><img class="publish-thumb" src="${esc(card.thumbnailUrl)}" alt="article thumbnail"></div>`
      : ""
  }
  <h3 class="publish-card-title">${esc(card.title)}</h3>
  <p class="approval-meta">Why it matters: ${esc(card.whyItMatters || "-")}</p>
  <p class="approval-meta">Target audience: ${esc(card.targetAudience || "-")}</p>
  <p class="approval-meta">Post angle: ${esc(card.postAngle || "-")}</p>
  <p class="approval-meta">Summary: ${esc(card.summary || card.postSummary || "-")}</p>
  <p class="approval-meta">Tone: ${esc(card.postTone || "-")}</p>
  <p class="publish-card-body">${esc(card.generatedPost || card.bodyPreview)}</p>
  <p class="approval-meta">Review notes: ${esc((card.reviewNotes || []).join(" | ") || "-")}</p>
  <p class="approval-meta">Source: <a class="publish-source-link" href="${esc(card.sourceLink)}" target="_blank" rel="noreferrer">${esc(
    card.sourceLink
  )}</a></p>
  <div class="publish-card-actions">
    <button type="button" class="crew-btn ghost" data-publish-action="edit" data-card-id="${esc(card.draftId)}">Edit</button>
    <button type="button" class="crew-btn primary" data-publish-action="approve" data-card-id="${esc(card.draftId)}" ${
      canApprovePublish ? "" : "disabled title=\"Telegram approval is required before Facebook publish\""
    }>Approve Publish</button>
    <button type="button" class="crew-btn ghost" data-publish-action="weekend" data-card-id="${esc(card.draftId)}">Save for Weekend Article</button>
    <button type="button" class="crew-btn danger" data-publish-action="cancel" data-card-id="${esc(card.draftId)}">Reject</button>
  </div>
  </div>
</article>`;
        }
      )
      .join("");
  }

  function renderApprovalList() {
    if (!(appState.growth.pendingApprovals || []).length) {
      return `<div class="approval-empty">No pending Telegram approvals.</div>`;
    }
    return appState.growth.pendingApprovals
      .map(
        (row) => `<article class="approval-item">
  <div class="approval-item-header">
    <span class="approval-username">@${esc(row.username || "unknown")}</span>
    <span class="approval-meta">ID: ${esc(row.user_id || "")}</span>
  </div>
  <div class="approval-meta">Followers: ${esc(row.followers || 0)} | Following: ${esc(row.following_count || 0)}</div>
  <div class="approval-actions">
    <button type="button" class="crew-btn primary" data-approval-action="approve" data-approval-user-id="${esc(row.user_id || "")}">Approve Follow</button>
    <button type="button" class="crew-btn ghost" data-approval-action="skip" data-approval-user-id="${esc(row.user_id || "")}">Skip</button>
    <button type="button" class="crew-btn danger" data-approval-action="block" data-approval-user-id="${esc(row.user_id || "")}">Block</button>
  </div>
</article>`
      )
      .join("");
  }

  async function saveFacebookRuntimeConfig() {
    try {
      const response = await apiJson("/api/crew/social/platform/facebook/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          app_id: appState.facebookAuthConfig.appId,
          app_secret: appState.facebookAuthConfig.appSecret,
          page_id: appState.facebookAuthConfig.pageId,
          user_long_lived_access_token: appState.facebookAuthConfig.userLongLivedAccessToken,
        }),
      });
      (response.logs || []).forEach(appendTagged);
      if (response.config) {
        appState.facebookAuthConfig.appId = String(response.config.app_id || "");
        appState.facebookAuthConfig.appSecret = String(response.config.app_secret || "");
        appState.facebookAuthConfig.pageId = String(response.config.page_id || "");
        appState.facebookAuthConfig.userLongLivedAccessToken = String(response.config.user_long_lived_access_token || "");
      }
      appState.xAuth = response.platform_auth || appState.xAuth;
      appendLog("system", "[AUTH] Facebook runtime config saved");
      renderModuleConfig();
    } catch (error) {
      appendApiError(error);
    }
  }

  async function runFacebookAdminReissue() {
    const shortToken = String(appState.facebookAuthConfig.shortUserAccessToken || "").trim();
    if (!shortToken) {
      appendLog("error", "[ERROR] Short-lived user token is required for admin reissue");
      return;
    }
    try {
      const response = await apiJson("/api/crew/social/platform/facebook/reissue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          app_id: appState.facebookAuthConfig.appId,
          app_secret: appState.facebookAuthConfig.appSecret,
          page_id: appState.facebookAuthConfig.pageId,
          user_short_lived_token: shortToken,
        }),
      });
      (response.logs || []).forEach(appendTagged);
      if (response.config) {
        appState.facebookAuthConfig.appId = String(response.config.app_id || appState.facebookAuthConfig.appId || "");
        appState.facebookAuthConfig.appSecret = String(response.config.app_secret || appState.facebookAuthConfig.appSecret || "");
        appState.facebookAuthConfig.pageId = String(response.config.page_id || appState.facebookAuthConfig.pageId || "");
        appState.facebookAuthConfig.userLongLivedAccessToken = String(
          response.config.user_long_lived_access_token || appState.facebookAuthConfig.userLongLivedAccessToken || ""
        );
      }
      appState.facebookAuthConfig.shortUserAccessToken = "";
      appState.xAuth = response.platform_auth || appState.xAuth;
      appendLog("system", "[AUTH] Facebook long-lived user token refreshed");
      renderModuleConfig();
    } catch (error) {
      appendApiError(error);
    }
  }

  function renderModuleConfig() {
    if (!dom.moduleConfigContent) return;
    if (!appState.selectedModule) {
      dom.moduleConfigContent.innerHTML =
        '<div class="module-config-empty">Select a module from Module Control Panel.</div>';
      return;
    }

    if (appState.selectedModule === "News Collector" && newsCollectorController) {
      newsCollectorController.renderModuleConfig();
      return;
    }

    if (appState.selectedModule === "Content") {
      const contentRows = (appState.content.queue || []).map(normalizeContentClip);
      const quality = appState.content.quality || {};
      const totalApproved = Number(quality.approved || 0);
      const totalRejected = Number(quality.rejected || 0);
      const modeOptions = (appState.content.modes || ["workconnect_clips"])
        .map((mode) => `<option value="${esc(mode)}" ${mode === appState.content.selectedMode ? "selected" : ""}>${esc(mode)}</option>`)
        .join("");
      dom.moduleConfigContent.innerHTML = `<div class="module-config-meta">
  Module: <span class="module-config-value">Content</span> |
  Queue: <span class="module-config-value">${esc(contentRows.length)}</span>
</div>
<div class="config-group">
  <label class="config-label" for="content-mode-select">Content Mode</label>
  <select id="content-mode-select" class="config-select">${modeOptions}</select>
  <p class="config-help">Current test mode stays inside Content pipeline. It generates a storyboard preview and sends it to Telegram for manual review.</p>
</div>
<div class="config-group">
  <label class="config-label">Generation</label>
  <div class="flow-control">
    <button type="button" class="crew-btn primary" id="content-generate-btn" ${appState.content.generationInFlight ? "disabled" : ""}>Generate ON</button>
    <button type="button" class="crew-btn ghost" id="content-refresh-btn">Refresh Queue</button>
  </div>
  <p class="config-help">WorkConnect Clips never auto-publish. Approvals and rejections are used to bias future quality selection.</p>
</div>
<div class="config-group">
  <label class="config-label">Quality Feedback</label>
  <div class="approval-meta">Approved: ${esc(totalApproved)} | Rejected: ${esc(totalRejected)}</div>
  <div class="approval-meta">Recent preference learning is applied to category and variant selection.</div>
</div>
<div class="config-group">
  <label class="config-label">Pending Clip Reviews</label>
  <div class="approval-list">${renderContentQueueList()}</div>
</div>`;
      return;
    }

    if (appState.selectedModule !== "Social") {
      dom.moduleConfigContent.innerHTML = `<div class="module-config-meta">Module: <span class="module-config-value">${esc(
        appState.selectedModule
      )}</span></div>
<div class="module-config-empty">Social module contains Facebook publishing controls.</div>`;
      return;
    }

    const xAccount = ((appState.accounts.default_social || {}).facebook_page || appState.socialForm.targetPage || "main_page").toString();
    const xAuth = appState.xAuth || {};
    const xAuthStatus = String(xAuth.status || "facebook_config_missing");
    const xAuthConnected = Boolean(xAuth.connected);
    const xAuthMessage = String(xAuth.message || "");
    const tokenStatus = String(xAuth.token_status || "");
    const tokenDetail = String(xAuth.token_detail || "");
    const tokenSource = String(xAuth.token_source || "");
    const authConfig = appState.facebookAuthConfig || {};
    const xAuthActionLabel = "Refresh Platform Status";
    const statusItems = ["idle", "collecting", "generating", "reviewing", "telegram_pending", "approved_for_publish", "rejected", "published", "error"];

    dom.moduleConfigContent.innerHTML = `<div class="module-config-meta">
  Module: <span class="module-config-value">Social</span> |
  Account: <span class="module-config-value">${esc(xAccount)}</span>
</div>
<div class="config-group">
  <label class="config-label">Platform</label>
  <select class="config-select" disabled>
    <option value="facebook" selected>Facebook</option>
    <option value="x">X (Experimental)</option>
  </select>
  <p class="config-help">Facebook is the default platform. X is kept as experimental.</p>
</div>
<div class="config-group">
  <label class="config-label" for="social-topic-input">Topic</label>
  <input id="social-topic-input" class="config-input" type="text" value="${esc(appState.socialForm.topic || "")}" placeholder="Optional topic">
</div>
<div class="config-group">
  <label class="config-label">News Source</label>
  <input class="config-input" type="text" value="${esc(appState.socialForm.newsSource || "naver")}" disabled>
</div>
<div class="config-group">
  <label class="config-label">Keywords Set</label>
  <input class="config-input" type="text" value="${esc(appState.socialForm.keywordSet || "foreign_workers_korea")}" disabled>
</div>
<div class="config-group">
  <label class="config-label">Mode</label>
  <input class="config-input" type="text" value="Review Before Publish (Telegram approval)" disabled>
</div>
<div class="config-group">
  <span class="config-label">Facebook Runtime Auth Config</span>
  <div class="approval-meta">These values are stored in UI runtime config. Runtime publish uses the saved long-lived user token and derives the page token automatically.</div>
  <input id="facebook-app-id-input" class="config-input" type="text" value="${esc(authConfig.appId || "")}" placeholder="Facebook App ID">
  <input id="facebook-app-secret-input" class="config-input" type="password" value="${esc(authConfig.appSecret || "")}" placeholder="Facebook App Secret">
  <input id="facebook-page-id-input" class="config-input" type="text" value="${esc(authConfig.pageId || "")}" placeholder="Facebook Page ID">
  <input id="facebook-user-token-input" class="config-input" type="password" value="${esc(authConfig.userLongLivedAccessToken || "")}" placeholder="Long-lived user token">
  <div class="flow-control">
    <button type="button" class="crew-btn primary" id="facebook-config-save-btn">Save Runtime Config</button>
    <button type="button" class="crew-btn ghost" id="x-auth-connect-btn">${esc(xAuthActionLabel)}</button>
  </div>
  <p class="config-help">Short-lived token is not stored. Enter it only when you need to refresh the long-lived user token.</p>
  <input id="facebook-short-user-token-input" class="config-input" type="password" value="${esc(authConfig.shortUserAccessToken || "")}" placeholder="Short-lived user token (manual reissue only)">
  <div class="flow-control">
    <button type="button" class="crew-btn ghost" id="facebook-token-reissue-btn">Refresh Long-Lived Token</button>
  </div>
</div>
<div class="config-group">
  <span class="config-label">Platform Connector</span>
  <div class="flow-control">
    <span class="flow-state-badge state-${esc(xAuthTone(xAuthStatus, xAuthConnected))}">${esc(xAuthLabel(xAuthStatus))}</span>
  </div>
  <p class="config-help">${esc(xAuthMessage || "Runtime publishing uses a derived Facebook Page token only.")}</p>
  <p class="config-help">Token Status: ${esc(tokenStatus || "unknown")}${tokenSource ? ` (${esc(tokenSource)})` : ""}</p>
  <p class="config-help">${esc(tokenDetail || "Manual admin reissue is required only when the long-lived user token expires or loses permissions.")}</p>
</div>
  <div class="flow-control">
    <span class="config-label">Generation Flow Control</span>
    <span class="flow-state-badge state-${esc(appState.generationState)}">${esc(flowLabel(appState.generationState))}</span>
  </div>
<div class="auto-generation-row">
  <span class="config-label">Auto Generation</span>
  <label class="crew-toggle" for="auto-generation-toggle">
    <input type="checkbox" id="auto-generation-toggle" ${appState.generationEnabled ? "checked" : ""}>
    <span class="crew-toggle-pill">${appState.generationEnabled ? "ON" : "OFF"}</span>
  </label>
</div>
<div class="generation-status-list">
  ${statusItems
    .map(
      (stateName) =>
        `<div class="generation-status-item ${appState.generationState === stateName ? "is-current" : ""}">${esc(
          stateName.replace("_", " ")
        )}</div>`
    )
    .join("")}
</div>
<section class="growth-automation">
  <span class="config-label">Last Post Monitoring</span>
  <div class="approval-meta">Current Post: ${esc(appState.currentPostId || "-")}</div>
  <div class="approval-meta">Created At: ${esc(appState.currentPostCreatedAt || "-")}</div>
  <div class="approval-meta">State: ${esc(JSON.stringify(appState.postMonitor || {}))}</div>
</section>
<section class="growth-automation">
  <span class="config-label">X Growth Automation</span>
  <div class="growth-input-row">
    <input id="growth-post-id-input" class="config-input" type="text" value="${esc(appState.growth.postId)}" placeholder="Post ID">
    <button type="button" class="crew-btn primary" id="growth-collect-btn">Collect Candidates</button>
  </div>
  <div class="growth-actions">
    <button type="button" class="crew-btn ghost" id="growth-refresh-btn">Refresh Pending</button>
    <span class="approval-meta">Like -> Filter -> Telegram -> Follow</span>
  </div>
  <div class="approval-list">${renderApprovalList()}</div>
</section>`;
  }

  function getActiveDraftCards() {
    return (appState.publishQueue || [])
      .map(normalizePublishDraft)
      .filter((item) => ["pending", "modified"].includes(String(item.approvalStatus || "").toLowerCase()));
  }

  function getApprovedDraftCards() {
    return (appState.publishQueue || [])
      .map(normalizePublishDraft)
      .filter((item) => String(item.approvalStatus || "").toLowerCase() === "approved" && !item.published);
  }

  function logDraftQueueIssues() {
    const activeCards = getActiveDraftCards();
    const firstErrorCard = activeCards.find((item) => String(item.lastTelegramError || "").trim());
    const firstPendingCard = activeCards.find((item) => !String(item.telegramSentAt || "").trim());
    const signature = firstErrorCard
      ? `error:${firstErrorCard.draftId}:${firstErrorCard.lastTelegramError}`
      : firstPendingCard
      ? `pending:${firstPendingCard.draftId}`
      : "";
    if (!signature || signature === appState.lastDraftIssueSignature) return;
    appState.lastDraftIssueSignature = signature;
    if (firstErrorCard) {
      appendLog("error", `[ERROR] Draft ${firstErrorCard.draftId} Telegram send failed: ${firstErrorCard.lastTelegramError}`);
      return;
    }
    if (firstPendingCard) {
      appendLog("system", `[SYSTEM] Draft ${firstPendingCard.draftId} is waiting for Telegram delivery/review.`);
    }
  }

  function savePreviewDraft() {
    if (!appState.preview.draft) return null;
    if (!dom.modalTitle || !dom.modalBody) return appState.preview.draft;
    if (dom.modalCategory) appState.preview.draft.category = String(dom.modalCategory.value || "").trim();
    if (dom.modalSourceLink) appState.preview.draft.source_link = String(dom.modalSourceLink.value || "").trim();
    if (dom.modalArticleTitle) appState.preview.draft.article_title = String(dom.modalArticleTitle.value || "").trim();
    appState.preview.draft.title = String(dom.modalTitle.value || "").trim();
    appState.preview.draft.body = String(dom.modalBody.value || "").trim();
    appState.preview.draft.body_preview =
      appState.preview.draft.body.length > 220 ? `${appState.preview.draft.body.slice(0, 220)}...` : appState.preview.draft.body;
    return appState.preview.draft;
  }

  function showPreviewModal(draft, context) {
    if (!dom.modal || !dom.modalTitle || !dom.modalBody) return;
    appState.preview.open = true;
    appState.preview.editing = false;
    appState.preview.draft = {
      draft_id: String(draft.draft_id || draft.draftId || "").trim(),
      platform: draft.platform || "x",
      category: String(draft.category || "").trim(),
      article_title: String(draft.article_title || "").trim(),
      source_link: String(draft.source_link || "").trim(),
      thumbnail_url: String(draft.thumbnail_url || "").trim(),
      cycle_id: String(draft.cycle_id || (context && context.cycleId) || "").trim(),
      title: String(draft.title || "").trim(),
      body: String(draft.body || "").trim(),
    };
    appState.preview.context = context || null;
    if (dom.modalCategory) dom.modalCategory.value = appState.preview.draft.category;
    if (dom.modalSourceLink) dom.modalSourceLink.value = appState.preview.draft.source_link;
    if (dom.modalArticleTitle) dom.modalArticleTitle.value = appState.preview.draft.article_title;
    if (dom.modalThumbnail && dom.modalThumbnailWrap) {
      const hasThumb = Boolean(appState.preview.draft.thumbnail_url);
      dom.modalThumbnailWrap.classList.toggle("is-hidden", !hasThumb);
      dom.modalThumbnail.src = hasThumb ? appState.preview.draft.thumbnail_url : "";
    }
    dom.modalTitle.value = appState.preview.draft.title;
    dom.modalBody.value = appState.preview.draft.body;
    dom.modalTitle.readOnly = true;
    dom.modalBody.readOnly = true;
    if (dom.modalEdit) dom.modalEdit.textContent = "Edit";
    dom.modal.classList.add("is-open");
    dom.modal.setAttribute("aria-hidden", "false");
  }

  function hidePreviewModal() {
    if (!dom.modal) return;
    appState.preview.open = false;
    appState.preview.editing = false;
    appState.preview.draft = null;
    appState.preview.context = null;
    dom.modal.classList.remove("is-open");
    dom.modal.setAttribute("aria-hidden", "true");
    if (dom.modalCategory) dom.modalCategory.value = "";
    if (dom.modalSourceLink) dom.modalSourceLink.value = "";
    if (dom.modalArticleTitle) dom.modalArticleTitle.value = "";
    if (dom.modalThumbnail && dom.modalThumbnailWrap) {
      dom.modalThumbnail.src = "";
      dom.modalThumbnailWrap.classList.add("is-hidden");
    }
    if (dom.modalTitle) dom.modalTitle.value = "";
    if (dom.modalBody) dom.modalBody.value = "";
    if (dom.modalEdit) dom.modalEdit.textContent = "Edit";
  }

  async function addDraftToPublishQueue(draft) {
    try {
      const resendTelegramReview = Boolean(draft && draft.resend_telegram_review);
      const response = await apiJson("/api/crew/social/publish-queue/add", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ draft, resend_telegram_review: resendTelegramReview }),
      });
      (response.logs || []).forEach(appendTagged);
      appState.publishQueue = response.publish_queue || [];
      updateGenerationState("telegram_pending");
      setActivity(["Draft sent to Telegram.", "Waiting for operator decision...", "Monitoring paused until resolution."]);
      renderPublishCards();
    } catch (error) {
      updateGenerationState("error");
      appendApiError(error);
    }
  }

  function renderPublishQueue() {
    renderPublishCards();
  }

  async function collectGrowthCandidates() {
    const postId = String(appState.growth.postId || "").trim();
    if (!postId) {
      appendLog("error", "[ERROR] Post ID is required for growth collection");
      return;
    }
    appendLog("growth", `[GROWTH] Collecting liker candidates for post ${postId}`);
    try {
      const response = await fetchLikingUsers(postId);
      (response.logs || []).forEach(appendTagged);
      appState.growth.pendingApprovals = response.pending_approvals || [];
      renderModuleConfig();
    } catch (error) {
      appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  async function refreshPendingApprovals(silent) {
    try {
      const response = await apiJson("/api/crew/growth/pending");
      appState.growth.pendingApprovals = response.pending_approvals || [];
      renderModuleConfig();
    } catch (error) {
      if (!silent) appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  async function refreshPublishQueue(silent) {
    try {
      const response = await apiJson("/api/crew/social/publish-queue");
      (response.logs || []).forEach(appendTagged);
      appState.publishQueue = response.publish_queue || [];
      logDraftQueueIssues();
      renderPublishCards();
    } catch (error) {
      if (!silent) appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  async function refreshContentQueue(silent) {
    try {
      const response = await apiJson("/api/crew/content/queue");
      (response.logs || []).forEach(appendTagged);
      appState.content.queue = response.content_queue || [];
      appState.content.quality = response.content_quality || appState.content.quality;
      renderModuleConfig();
    } catch (error) {
      if (!silent) appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  async function generateContentClip() {
    if (appState.content.generationInFlight) return;
    appState.content.generationInFlight = true;
    setModuleStatus("Content", "running", true);
    appendLog("agent", "[AGENT] Content Agent started WorkConnect Clips generation");
    setActivity([
      "Selecting a WorkConnect Clips topic...",
      "Building script and preview assets...",
      "Sending clip review to Telegram...",
    ]);
    try {
      const response = await apiJson("/api/crew/content/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          selected_mode: appState.content.selectedMode,
        }),
      });
      (response.logs || []).forEach(appendTagged);
      appState.content.queue = response.content_queue || appState.content.queue;
      appState.content.quality = response.content_quality || appState.content.quality;
      const clip = response.clip || {};
      if (String(clip.last_telegram_error || "").trim()) {
        setModuleStatus("Content", "error", true);
        setActivity([
          "WorkConnect Clip preview generated.",
          "Telegram delivery failed for this clip.",
          "Check queue or retry after network is available.",
        ]);
      } else {
        setModuleStatus("Content", "complete", true);
        setActivity([
          "WorkConnect Clip generated.",
          "Telegram review sent for manual approval.",
          "Approval history will bias future variants.",
        ]);
      }
      renderModuleConfig();
    } catch (error) {
      setModuleStatus("Content", "error", true);
      appendApiError(error);
    } finally {
      appState.content.generationInFlight = false;
    }
  }

  async function applyApprovalDecision(userId, decision) {
    try {
      const response = await apiJson("/api/crew/telegram/decision", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: userId, decision }),
      });
      (response.logs || []).forEach(appendTagged);
      await refreshPendingApprovals(true);
      renderModuleConfig();
    } catch (error) {
      appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  function shouldFetchLikers(likeCount, state) {
    const currentLikeCount = Number(likeCount || 0);
    const monitorState = state || {};
    const lastFetchCount = Number(monitorState.last_fetch_count || 0);
    const lastFetchAt = monitorState.last_fetch_at ? new Date(monitorState.last_fetch_at).getTime() : null;
    if (currentLikeCount < 100) return { shouldFetch: false, reason: "below_threshold" };
    if (!lastFetchAt) return { shouldFetch: true, reason: "first_threshold_hit" };
    if (currentLikeCount - lastFetchCount >= 20) return { shouldFetch: true, reason: "delta_20_plus" };
    if (Date.now() - lastFetchAt >= 30 * 60 * 1000) return { shouldFetch: true, reason: "30m_elapsed" };
    return { shouldFetch: false, reason: "not_needed" };
  }

  async function fetchLikingUsers(postId) {
    return apiJson("/api/crew/growth/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ post_id: postId, limit: 20 }),
    });
  }

  function dedupeSavedLikers(postId, likers) {
    if (!appState.localLikerCache[postId]) appState.localLikerCache[postId] = {};
    const bucket = appState.localLikerCache[postId];
    return (likers || []).filter((row) => {
      const userId = String((row && row.id) || "").trim();
      if (!userId) return false;
      if (bucket[userId]) return false;
      return true;
    });
  }

  function saveNewLikers(postId, newLikers) {
    if (!appState.localLikerCache[postId]) appState.localLikerCache[postId] = {};
    const bucket = appState.localLikerCache[postId];
    let saved = 0;
    (newLikers || []).forEach((row) => {
      const userId = String((row && row.id) || "").trim();
      if (!userId || bucket[userId]) return;
      bucket[userId] = true;
      saved += 1;
    });
    return saved;
  }

  async function monitorCurrentPost() {
    const response = await apiJson("/api/crew/social/monitor/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    (response.logs || []).forEach((message) => {
      const text = String(message || "");
      if (text.includes("[SYSTEM] No current post to monitor")) {
        const now = Date.now();
        if (now - Number(appState.loop.lastNoPostLogAt || 0) < 55 * 1000) return;
        appState.loop.lastNoPostLogAt = now;
      }
      appendTagged(text);
    });
    appState.currentPostId = response.post_id || null;
    appState.currentPostCreatedAt = response.current_post_created_at || null;
    appState.postMonitor = response.monitor_state || appState.postMonitor;
    renderModuleConfig();
    return response;
  }

  async function beforeGenerateNextPost() {
    const response = await apiJson("/api/crew/social/monitor/finalize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    (response.logs || []).forEach(appendTagged);
    appState.postMonitor = response.monitor_state || appState.postMonitor;
    renderModuleConfig();
    return response;
  }

  function closePostMonitor(postId) {
    if (!postId) return;
    if (!appState.postMonitor) appState.postMonitor = {};
    appState.postMonitor.closed = true;
    appState.postMonitor.monitoring = false;
  }

  async function registerPublishedPost(postId) {
    if (!postId) return null;
    const response = await apiJson("/api/crew/social/register-published", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ post_id: postId }),
    });
    appState.currentPostId = response.post_id || postId;
    appState.currentPostCreatedAt = response.current_post_created_at || new Date().toISOString();
    appState.postMonitor = response.monitor_state || appState.postMonitor;
    renderModuleConfig();
    return response;
  }

  async function runGenerationCycle(mode, opts) {
    const phase = String(mode || "normal");
    const manualRun = phase === "manual";
    if ((!appState.generationEnabled && !manualRun) || appState.loop.cycleInFlight) return;
    appState.loop.cycleInFlight = true;
    try {
      const cycleId = String((opts && opts.cycleId) || appState.loop.currentCycleId || `cycle_${Date.now()}`);
      appState.loop.currentCycleId = cycleId;
      if (phase === "bootstrap") {
        updateGenerationState("bootstrap");
      }
      updateGenerationState("generating");
      appendLog("agent", "[AGENT] Draft generation started");
      appendLog("agent", "[AGENT] Collecting Facebook news candidate pool");
      setActivity([
        "Collecting news...",
        "Reviewing relevance with local LLM...",
        "Generating Facebook draft...",
      ]);

      const response = await apiJson("/api/crew/social/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          phase,
          platform: "facebook",
          topic: appState.socialForm.topic,
          tone: appState.socialForm.tone,
          length: appState.socialForm.length,
          news_source: appState.socialForm.newsSource,
          keyword_set: appState.socialForm.keywordSet,
          options: appState.socialForm.options,
          cycle_id: cycleId,
        }),
      });
      (response.logs || []).forEach(appendTagged);
      if (response.no_candidate) {
        const responseWaitSec = Number(response.wait_seconds || 0);
        if (phase === "bootstrap") {
          updateGenerationState("bootstrap");
        } else {
          updateGenerationState("monitoring");
        }
        appState.loop.currentCycleId = null;
        appState.loop.nextCycleAt = Date.now() + (responseWaitSec > 0 ? responseWaitSec * 1000 : appState.loop.generationIntervalMs);
        if (responseWaitSec > 0) {
          const waitMinutes = Math.max(1, Math.ceil(responseWaitSec / 60));
          appendLog("system", `[SYSTEM] Generation cooldown active. Waiting about ${waitMinutes} minute(s) before next cycle`);
          setActivity([
            "Generation cooldown is active after the recent publish.",
            `Waiting about ${waitMinutes} minute(s) before the next article lookup...`,
            "Monitoring last post...",
          ]);
        } else {
          appendLog("system", "[SYSTEM] Waiting 10 minutes before next cycle");
          setActivity(["No new article candidate.", "Waiting 10 minutes before next cycle...", "Monitoring last post..."]);
        }
        return;
      }
      if (!response.draft) throw new Error("missing draft from generation response");
      const responseCycle = String(response.cycle_id || cycleId);
      appState.loop.currentCycleId = responseCycle;
      appState.publishQueue = response.publish_queue || appState.publishQueue;
      renderPublishCards();
      updateGenerationState("telegram_pending");
      setActivity(["Draft sent to Telegram.", "Waiting for operator decision...", "UI is status-only until approval."]);
      appState.loop.lastGenerationAt = Date.now();
    } catch (error) {
      updateGenerationState("error");
      appendApiError(error);
    } finally {
      appState.loop.cycleInFlight = false;
    }
  }

  function getGenerationGateStatus() {
    if (!appState.generationEnabled || !appState.loop.running) return { canRun: false, reason: "loop_disabled" };
    if (appState.loop.cycleInFlight) return { canRun: false, reason: "cycle_in_flight" };
    if (getActiveDraftCards().length > 0) return { canRun: false, reason: "publish_queue_pending" };
    if (getApprovedDraftCards().length > 0) return { canRun: false, reason: "approved_for_publish" };
    if (appState.generationState === "waiting_approval") return { canRun: false, reason: "waiting_approval" };
    if (appState.generationState === "telegram_pending") return { canRun: false, reason: "telegram_pending" };
    if (appState.generationState === "approved_for_publish") return { canRun: false, reason: "approved_for_publish" };
    if (appState.generationState === "publishing_review") return { canRun: false, reason: "publishing_review" };
    const nextCycleAt = Number(appState.loop.nextCycleAt || 0);
    const now = Date.now();
    if (now < nextCycleAt) {
      const waitMs = Math.max(0, nextCycleAt - now);
      const waitMin = Math.max(1, Math.ceil(waitMs / 60000));
      return { canRun: false, reason: "next_cycle_wait", waitMin };
    }
    return { canRun: true, reason: "ready", waitMin: 0 };
  }

  function canRunGenerationCycle() {
    return getGenerationGateStatus().canRun;
  }

  async function runLoopTick() {
    if (!appState.generationEnabled || !appState.loop.running || appState.loop.tickInFlight) return;
    appState.loop.tickInFlight = true;
    try {
      await refreshPublishQueue(true);
      const approvedCards = getApprovedDraftCards();
      if (approvedCards.length > 0) {
        const approvedCard = approvedCards[0];
        updateGenerationState("approved_for_publish");
        setActivity([
          "Telegram approval received.",
          `Publishing ${approvedCard.draftId} to Facebook...`,
          "Auto generation resumes after publish.",
        ]);
        await approvePublish(approvedCard.draftId);
        return;
      }
      if (["waiting_approval", "publishing_review", "telegram_pending", "approved_for_publish"].includes(appState.generationState)) {
        const activeCards = getActiveDraftCards();
        if (activeCards.length > 0) {
          const waitingOn = activeCards[0];
          setActivity([
            "Queued draft still unresolved.",
            waitingOn.telegramSentAt
              ? `Waiting for Telegram decision on ${waitingOn.draftId}.`
              : `Telegram delivery pending for ${waitingOn.draftId}; auto-retry will continue.`,
            "Auto generation remains paused until draft is resolved or expires.",
          ]);
          return;
        }
        if (appState.generationState === "telegram_pending" || appState.generationState === "approved_for_publish") {
          updateGenerationState(appState.currentPostId ? "monitoring" : "bootstrap");
        }
      }
      const hadKnownPost = Boolean(appState.currentPostId);
      const previousNextCycleAt = Number(appState.loop.nextCycleAt || 0);
      const monitor = await monitorCurrentPost();
      const hasCurrentPost = Boolean((monitor && monitor.post_id) || appState.currentPostId);
      if (!hasCurrentPost) {
        // Unlock bootstrap immediately only when we just lost a previously tracked post
        // or this is the very first bootstrap tick.
        appState.currentPostId = null;
        appState.currentPostCreatedAt = null;
        const shouldUnlockBootstrapNow = hadKnownPost || !previousNextCycleAt;
        if (shouldUnlockBootstrapNow) {
          appState.loop.nextCycleAt = Date.now();
        }
        updateGenerationState("bootstrap");
        const gate = getGenerationGateStatus();
        if (gate.canRun) {
          appendLog("system", "[SYSTEM] Starting bootstrap generation");
          setActivity([
            "No current post found.",
            "Starting bootstrap generation...",
            "Building first Korea Update draft...",
          ]);
          await runGenerationCycle("bootstrap");
        } else {
          const now = Date.now();
          const waitMin = Number(gate.waitMin || 1);
          if (now - Number(appState.loop.lastBootstrapDeferredLogAt || 0) >= 55 * 1000) {
            appState.loop.lastBootstrapDeferredLogAt = now;
            appendLog("system", `[SYSTEM] Bootstrap generation deferred: ${gate.reason}`);
          }
          setActivity([
            "No current post found.",
            `Waiting reason: ${gate.reason}`,
            `Next bootstrap check in about ${waitMin} minute(s).`,
          ]);
        }
      } else {
        updateGenerationState("monitoring");
        const gate = getGenerationGateStatus();
        if (gate.canRun) {
          setActivity(["Monitoring last post...", "Checking like count...", "Starting generation cycle now..."]);
          await runGenerationCycle("normal");
        } else {
          const now = Date.now();
          const waitMin = Number(gate.waitMin || 1);
          if (now - Number(appState.loop.lastGateLogAt || 0) >= 55 * 1000) {
            appState.loop.lastGateLogAt = now;
            appendLog("system", `[SYSTEM] Generation waiting: ${gate.reason}${gate.reason === "next_cycle_wait" ? ` (${waitMin}m)` : ""}`);
          }
          setActivity([
            "Monitoring last post...",
            `Waiting reason: ${gate.reason}`,
            gate.reason === "next_cycle_wait" ? `Next cycle in about ${waitMin} minute(s).` : "Generation will resume automatically.",
          ]);
        }
      }
    } catch (error) {
      updateGenerationState("error");
      appendApiError(error);
    } finally {
      appState.loop.tickInFlight = false;
      renderModuleConfig();
    }
  }

  function startGenerationLoop() {
    if (appState.loop.running) return;
    appState.loop.running = true;
    if (!appState.preview.open) appState.loop.currentCycleId = null;

    if (appState.currentPostId) {
      appState.loop.nextCycleAt = Date.now() + appState.loop.generationIntervalMs;
    } else {
      appState.loop.nextCycleAt = Date.now();
    }

    if (appState.currentPostId) {
      updateGenerationState("monitoring");
      setActivity(["Auto generation loop enabled.", "Monitoring current post every 1 minute.", "Waiting for next generation window."]);
    } else {
      updateGenerationState("bootstrap");
      setActivity(["Auto generation loop enabled.", "No current post detected.", "Preparing bootstrap generation."]);
    }
    appendLog("system", "[SYSTEM] Auto generation enabled");

    if (appState.loop.timer) window.clearInterval(appState.loop.timer);
    appState.loop.timer = window.setInterval(runLoopTick, MONITOR_INTERVAL_MS);
    runLoopTick();
  }

  function stopGenerationLoop() {
    if (appState.loop.timer) {
      window.clearInterval(appState.loop.timer);
      appState.loop.timer = null;
    }
    appState.loop.running = false;
    appState.loop.tickInFlight = false;
    appState.loop.cycleInFlight = false;
    appState.loop.nextCycleAt = null;
    appState.loop.currentCycleId = null;
    updateGenerationState("stopped");
    appendLog("system", "[SYSTEM] Auto generation disabled");
    setActivity(["Generation loop stopped.", "Drafts and publish review queue retained.", "Monitoring paused."]);
  }

  async function toggleAutoGeneration(enabled) {
    const wantEnable = Boolean(enabled);
    if (!wantEnable) {
      appState.generationEnabled = false;
      appState.oauth.pendingAutoEnable = false;
      stopGenerationLoop();
      renderModuleConfig();
      persistUiButtonState();
      return;
    }

    appendLog("system", "[SYSTEM] Auto generation preflight started");
    const preflight = await validateAutoGenerationPreflight();
    if (!preflight.ok) {
      appState.generationEnabled = false;
      if (appState.loop.running) {
        stopGenerationLoop();
      } else {
        updateGenerationState("stopped");
      }
      appendLog("error", `[ERROR] Auto generation blocked: ${preflight.reason}`);
      if (preflight.message) {
        appendLog("error", `[ERROR] ${preflight.message}`);
      }
      appendLog("system", "[SYSTEM] Auto generation remains OFF");
      if (String(preflight.reason || "").startsWith("x_auth_")) {
        appState.oauth.pendingAutoEnable = true;
        appendLog("error", "[ERROR] Facebook runtime publish is not ready.");
        appendLog("system", "[SYSTEM] Browser OAuth flow is disabled in runtime publish mode.");
        appendLog("system", "[SYSTEM] Run the admin/manual reissue flow, then refresh platform status.");
      }
      setActivity([
        "Auto generation preflight failed.",
        `Reason: ${preflight.reason}`,
        "Set Facebook Page credentials, then enable Auto Generation again.",
      ]);
      renderModuleConfig();
      persistUiButtonState();
      return;
    }

    appState.generationEnabled = true;
    appState.oauth.pendingAutoEnable = false;
    appendLog("system", "[SYSTEM] Auto generation preflight passed");
    startGenerationLoop();
    renderModuleConfig();
    persistUiButtonState();
  }

  function startGenerationLoopAlias() {
    startGenerationLoop();
  }

  function stopGenerationLoopAlias() {
    stopGenerationLoop();
  }

  function resumeMonitoringAfterPublish() {
    appState.loop.nextCycleAt = Date.now() + appState.loop.generationIntervalMs;
    appState.loop.currentCycleId = null;
    if (appState.generationEnabled) {
      if (!appState.loop.running) startGenerationLoop();
      updateGenerationState("monitoring");
      const waitMinutes = Math.max(1, Math.round(appState.loop.generationIntervalMs / 60000));
      appendLog("system", `[SYSTEM] Waiting ${waitMinutes} minutes before next cycle`);
      setActivity(["Publish completed.", "Monitoring new current post.", `Waiting ${waitMinutes} minutes before next cycle...`]);
    } else {
      updateGenerationState("stopped");
    }
    renderModuleConfig();
  }

  async function approvePublish(cardId) {
    const card = (appState.publishQueue || []).map(normalizePublishDraft).find((item) => item.draftId === cardId);
    if (!card) return;
    if (String(card.approvalStatus || "").toLowerCase() !== "approved") {
      appendLog("error", `[ERROR] Approve blocked: recommendation=${card.finalRecommendation} approval_status=${card.approvalStatus}`);
      return;
    }
    try {
      const response = await apiJson("/api/crew/social/publish-queue/approve", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ draft_id: card.draftId }),
      });
      (response.logs || []).forEach(appendTagged);
      appState.publishQueue = response.publish_queue || [];
      const publishResult = response.publish_result || {};
      const monitor = publishResult.monitor || {};
      const postId = ((publishResult.result || {}).post_id) || monitor.post_id || null;
      if (postId) {
        appState.currentPostId = postId;
        appState.currentPostCreatedAt = monitor.current_post_created_at || new Date().toISOString();
        appState.postMonitor = monitor.monitor_state || appState.postMonitor;
      }
      updateGenerationState("published");
      renderPublishCards();
      resumeMonitoringAfterPublish();
    } catch (error) {
      updateGenerationState("error");
      appendApiError(error);
    }
  }

  function cancelPublish(cardId) {
    apiJson("/api/crew/social/publish-queue/cancel", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ draft_id: cardId }),
    })
      .then((response) => {
        (response.logs || []).forEach(appendTagged);
        appState.publishQueue = response.publish_queue || [];
        renderPublishCards();
        if (appState.generationEnabled) {
          updateGenerationState("monitoring");
          appState.loop.nextCycleAt = Date.now() + appState.loop.generationIntervalMs;
        } else {
          updateGenerationState("stopped");
        }
      })
      .catch((error) => appendLog("error", `[ERROR] ${String(error.message || error)}`));
  }

  function saveForWeekendArticle(cardId) {
    apiJson("/api/crew/social/publish-queue/weekend", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ draft_id: cardId }),
    })
      .then((response) => {
        (response.logs || []).forEach(appendTagged);
        appState.publishQueue = response.publish_queue || [];
        renderPublishCards();
        appendLog("system", `[SYSTEM] Saved for weekend article: ${cardId}`);
      })
      .catch((error) => appendLog("error", `[ERROR] ${String(error.message || error)}`));
  }

  function handleDynamicClick(event) {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (newsCollectorController && newsCollectorController.handleDynamicClick(event)) return;

    const publishToggle = target.dataset.publishToggle;
    if (publishToggle === "card") {
      const cardId = String(target.dataset.cardId || "").trim();
      if (!cardId) return;
      const current = appState.publishCardCollapse[cardId] !== false;
      appState.publishCardCollapse[cardId] = !current;
      renderPublishCards();
      return;
    }

    const lengthOption = target.dataset.lengthOption;
    if (lengthOption) {
      appState.socialForm.length = lengthOption;
      const profile = appState.platformProfiles.facebook || {};
      appState.platformConfig.facebook = {
        format: profile.format || "post",
        tone: appState.socialForm.tone,
        length: lengthOption,
      };
      renderModuleConfig();
      return;
    }

    if (target.id === "growth-collect-btn") {
      collectGrowthCandidates();
      return;
    }
    if (target.id === "growth-refresh-btn") {
      refreshPendingApprovals(false);
      return;
    }
    if (target.id === "facebook-config-save-btn") {
      saveFacebookRuntimeConfig();
      return;
    }
    if (target.id === "facebook-token-reissue-btn") {
      runFacebookAdminReissue();
      return;
    }
    if (target.id === "x-auth-connect-btn") {
      connectXOAuth();
      return;
    }
    if (target.id === "content-generate-btn") {
      generateContentClip();
      return;
    }
    if (target.id === "content-refresh-btn") {
      refreshContentQueue(false);
      return;
    }

    const publishAction = target.dataset.publishAction;
    if (publishAction) {
      const cardId = target.dataset.cardId;
      if (!cardId) return;
      if (publishAction === "edit") {
        const card = (appState.publishQueue || []).map(normalizePublishDraft).find((item) => item.draftId === cardId);
        if (!card) return;
        showPreviewModal(
          {
            draft_id: card.draftId,
            platform: "facebook",
            category: card.category,
            article_title: card.articleTitle,
            original_title: card.articleTitle,
            source_link: card.sourceLink,
            thumbnail_url: card.thumbnailUrl,
            cycle_id: card.cycleId,
            title: card.title,
            body: card.body,
          },
          { source: "publish-card", cardId }
        );
        return;
      }
      if (publishAction === "approve") {
        approvePublish(cardId);
        return;
      }
      if (publishAction === "weekend") {
        saveForWeekendArticle(cardId);
        return;
      }
      if (publishAction === "cancel") {
        cancelPublish(cardId);
      }
      return;
    }

    const approvalAction = target.dataset.approvalAction;
    if (approvalAction) {
      const userId = target.dataset.approvalUserId;
      if (!userId) return;
      applyApprovalDecision(userId, approvalAction);
      return;
    }

    const contentAction = String(target.dataset.contentAction || "").trim().toLowerCase();
    if (contentAction) {
      const clipId = String(target.dataset.clipId || "").trim();
      if (!clipId) return;
      resolveContentClip(clipId, contentAction);
    }
  }

  function handleDynamicChange(event) {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;

    if (target.id === "auto-generation-toggle" && target instanceof HTMLInputElement) {
      toggleAutoGeneration(target.checked);
      return;
    }
    if (target.id === "social-tone-select" && target instanceof HTMLSelectElement) {
      appState.socialForm.tone = target.value;
      const profile = appState.platformProfiles.facebook || {};
      appState.platformConfig.facebook = {
        format: profile.format || "post",
        tone: target.value,
        length: appState.socialForm.length,
      };
      return;
    }
    if (target.id === "content-mode-select" && target instanceof HTMLSelectElement) {
      appState.content.selectedMode = target.value;
      persistUiButtonState();
      return;
    }

    if (target.dataset.optionKey && target instanceof HTMLInputElement) {
      appState.socialForm.options[target.dataset.optionKey] = target.checked;
      return;
    }
  }

  function handleDynamicInput(event) {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (newsCollectorController && newsCollectorController.handleDynamicInput(event)) return;
    if (target.id === "social-topic-input" && target instanceof HTMLInputElement) {
      appState.socialForm.topic = target.value;
      return;
    }
    if (target.id === "growth-post-id-input" && target instanceof HTMLInputElement) {
      appState.growth.postId = target.value;
      return;
    }
    if (target.id === "facebook-app-id-input" && target instanceof HTMLInputElement) {
      appState.facebookAuthConfig.appId = target.value;
      return;
    }
    if (target.id === "facebook-app-secret-input" && target instanceof HTMLInputElement) {
      appState.facebookAuthConfig.appSecret = target.value;
      return;
    }
    if (target.id === "facebook-page-id-input" && target instanceof HTMLInputElement) {
      appState.facebookAuthConfig.pageId = target.value;
      return;
    }
    if (target.id === "facebook-user-token-input" && target instanceof HTMLInputElement) {
      appState.facebookAuthConfig.userLongLivedAccessToken = target.value;
      return;
    }
    if (target.id === "facebook-short-user-token-input" && target instanceof HTMLInputElement) {
      appState.facebookAuthConfig.shortUserAccessToken = target.value;
    }
  }

  async function loadBootstrap() {
    try {
      const response = await apiJson("/api/crew/social/config");
      appState.platformProfiles = response.platform_profiles || appState.platformProfiles;
      appState.accounts = response.accounts || appState.accounts;
      appState.xAuth = response.x_auth || appState.xAuth;
      appState.facebookAuthConfig = {
        appId: String(((response.facebook_auth_config || {}).app_id) || ""),
        appSecret: String(((response.facebook_auth_config || {}).app_secret) || ""),
        pageId: String(((response.facebook_auth_config || {}).page_id) || ""),
        userLongLivedAccessToken: String(((response.facebook_auth_config || {}).user_long_lived_access_token) || ""),
        shortUserAccessToken: "",
      };
      appState.content.modes = response.content_modes || appState.content.modes;
      appState.content.selectedMode = String(((response.content_config || {}).selected_mode) || appState.content.selectedMode || "workconnect_clips");
      appState.content.queue = response.content_queue || [];
      appState.content.quality = response.content_quality || appState.content.quality;
      appState.currentPostId = response.current_post_id || null;
      appState.currentPostCreatedAt = response.current_post_created_at || null;
      if (response.next_cycle_at) {
        const parsedNextCycle = Date.parse(response.next_cycle_at);
        if (Number.isFinite(parsedNextCycle)) {
          appState.loop.nextCycleAt = parsedNextCycle;
        }
      }
      appState.publishQueue = response.publish_queue || [];
      const waitSec = Number(response.post_publish_wait_sec || 0);
      if (waitSec > 0) {
        appState.loop.generationIntervalMs = waitSec * 1000;
      }
      const profile = appState.platformProfiles.facebook || {};
      appState.socialForm.tone = profile.tone || appState.socialForm.tone;
      appState.socialForm.length = profile.length || appState.socialForm.length;
      appState.platformConfig.facebook = {
        format: profile.format || "post",
        tone: appState.socialForm.tone,
        length: appState.socialForm.length,
      };
      if (appState.currentPostId) {
        appState.growth.postId = appState.currentPostId;
      }
      (response.publish_queue_logs || []).forEach(appendTagged);
      if (getActiveDraftCards().length > 0) {
        updateGenerationState("telegram_pending");
        setActivity([
          "Queued draft found on startup.",
          "Telegram delivery / approval status is being monitored.",
          "Auto generation stays paused until the draft is resolved or expires.",
        ]);
      }
      clearOAuthAuthorizePending();
      try {
        localStorage.removeItem("crew_x_oauth_callback_result");
      } catch (error) {
        appendLog("error", `[ERROR] oauth callback cache clear failed: ${String(error.message || error)}`);
      }
      logDraftQueueIssues();
      renderModuleConfig();
      renderPublishCards();
    } catch (error) {
      appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  async function refreshXOAuthStatus(logResult) {
    try {
      const response = await apiJson("/api/crew/social/platform/status");
      appState.xAuth = response.platform_auth || response.x_auth || appState.xAuth;
      if (logResult) {
        const statusDetail = String(
          (appState.xAuth && (appState.xAuth.detail || appState.xAuth.message)) || ""
        );
        appendLog(
          appState.xAuth.connected ? "system" : "error",
          `[AUTH] Platform Status: ${xAuthLabel(appState.xAuth.status)}${statusDetail ? ` - ${statusDetail}` : ""}`
        );
      }
      renderModuleConfig();
    } catch (error) {
      appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  async function validateAutoGenerationPreflight() {
    try {
      const response = await apiJson("/api/crew/social/platform/status");
      appState.xAuth = response.platform_auth || response.x_auth || appState.xAuth;
    } catch (error) {
      return {
        ok: false,
        reason: "x_auth_check_failed",
        message: String(error && error.message ? error.message : error),
      };
    }

    if (!appState.xAuth || !appState.xAuth.connected) {
      const status = String((appState.xAuth && appState.xAuth.status) || "facebook_config_missing");
      const message = String(
        (appState.xAuth && (appState.xAuth.detail || appState.xAuth.message)) || "Missing Facebook Page credentials."
      );
      return {
        ok: false,
        reason: `x_auth_${status}`,
        message,
      };
    }
    return { ok: true };
  }

  function handleOAuthCallbackPayload(payload) {
    const row = payload || {};
    const logs = Array.isArray(row.logs) ? row.logs : [];
    logs.forEach(appendTagged);
    if (row && row.type === "x_oauth_connected") {
      clearOAuthAuthorizePending();
      appendLog("system", "[AUTH] Callback phase reached (post-callback)");
      appendLog("system", "[AUTH] Legacy callback completed");
      refreshXOAuthStatus(true).then(() => {
        if (appState.oauth.pendingAutoEnable && appState.xAuth && appState.xAuth.connected) {
          appendLog("system", "[SYSTEM] Platform status refreshed. Resuming auto generation.");
          toggleAutoGeneration(true);
        }
      });
      return;
    }
    if (row && row.type === "x_oauth_failed") {
      clearOAuthAuthorizePending();
      appendLog("system", "[AUTH] Callback phase reached (post-callback)");
      appendLog("error", "[ERROR] Legacy callback failed");
      refreshXOAuthStatus(true);
    }
  }

  function drainOAuthCallbackCache() {
    const key = "crew_x_oauth_callback_result";
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return;
      localStorage.removeItem(key);
      const payload = JSON.parse(raw);
      handleOAuthCallbackPayload(payload);
    } catch (error) {
      appendLog("error", `[ERROR] oauth callback cache parse failed: ${String(error.message || error)}`);
    }
  }

  function setOAuthAuthorizePending(payload) {
    const key = "crew_x_oauth_authorize_pending";
    try {
      localStorage.setItem(key, JSON.stringify(payload || {}));
    } catch (error) {
      appendLog("error", `[ERROR] oauth pending cache write failed: ${String(error.message || error)}`);
    }
  }

  function clearOAuthAuthorizePending() {
    const key = "crew_x_oauth_authorize_pending";
    try {
      localStorage.removeItem(key);
    } catch (error) {
      appendLog("error", `[ERROR] oauth pending cache clear failed: ${String(error.message || error)}`);
    }
  }

  function inspectOAuthAuthorizePending() {
    const key = "crew_x_oauth_authorize_pending";
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return;
      const row = JSON.parse(raw);
      const startedAt = Number(row.started_at || 0);
      const elapsedSec = startedAt > 0 ? Math.max(0, Math.floor((Date.now() - startedAt) / 1000)) : 0;
      const host = String(row.authorize_host || "-");
      const redirectUri = String(row.redirect_uri || "-");
      appendLog("system", `[AUTH] OAuth authorize pending (pre-callback): ${elapsedSec}s elapsed`);
      appendLog("system", `[AUTH] authorize_host=${host}, redirect_uri=${redirectUri}`);
      if (elapsedSec >= 30) {
        appendLog(
          "error",
          "[ERROR] No callback detected yet. If Connected App is not created, failure is likely before callback (authorize/consent stage)."
        );
        appendLog(
          "error",
          "[ERROR] authorize_consent_completion_failure (pre-callback): X consent was not completed, so callback redirect did not occur."
        );
      }
    } catch (error) {
      appendLog("error", `[ERROR] oauth pending cache parse failed: ${String(error.message || error)}`);
    }
  }

  function maskSuffix(value, keep) {
    const text = String(value || "").trim();
    const size = Number(keep || 8);
    if (!text) return "-";
    if (text.length <= size) return text;
    return `...${text.slice(-size)}`;
  }

  function clearOAuthCallbackQuery() {
    const url = new URL(window.location.href);
    const keys = [
      "x_oauth_code",
      "x_oauth_state",
      "x_oauth_error",
      "x_oauth_error_description",
      "x_oauth_callback_path",
      "x_oauth_query_state",
      "x_oauth_query_code_present",
    ];
    let changed = false;
    keys.forEach((key) => {
      if (url.searchParams.has(key)) {
        url.searchParams.delete(key);
        changed = true;
      }
    });
    if (!changed) return;
    const next = `${url.pathname}${url.search ? url.search : ""}${url.hash ? url.hash : ""}`;
    window.history.replaceState({}, document.title, next);
  }

  async function consumeOAuthCallbackFromUrl() {
    const url = new URL(window.location.href);
    const params = url.searchParams;
    const code = String(params.get("x_oauth_code") || "").trim();
    const state = String(params.get("x_oauth_state") || "").trim();
    const error = String(params.get("x_oauth_error") || "").trim();
    const errorDescription = String(params.get("x_oauth_error_description") || "").trim();
    const callbackPath = String(params.get("x_oauth_callback_path") || "").trim();
    const queryCodePresent = String(params.get("x_oauth_query_code_present") || "").trim();
    const queryState = String(params.get("x_oauth_query_state") || "").trim();
    const hasCallbackPayload = Boolean(
      code || state || error || callbackPath || params.has("x_oauth_query_code_present") || params.has("x_oauth_query_state")
    );
    if (!hasCallbackPayload) return;

    appendLog("system", "[AUTH] Callback received");
    appendLog("system", "[AUTH] Callback phase reached (post-callback)");
    appendLog("system", `[AUTH] callback_path=${callbackPath || "/api/crew/social/x/oauth/callback"}`);
    appendLog("system", `[AUTH] query_code_present=${queryCodePresent || String(Boolean(code))}`);
    appendLog("system", `[AUTH] query_state=${maskSuffix(queryState || state, 10)}`);

    try {
      if (error) {
        appendLog("error", `[ERROR] OAuth callback error: ${error}`);
        if (errorDescription) appendLog("error", `[ERROR] ${errorDescription}`);
        return;
      }
      if (!code || !state) {
        appendLog("error", "[ERROR] OAuth callback missing code/state");
        return;
      }

      appendLog("system", "[AUTH] Token exchange started");
      const response = await apiJson("/api/crew/social/x/oauth/exchange", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code, state }),
      });
      (response.logs || []).forEach(appendTagged);
      appendLog("system", "[AUTH] Token saved");
      appState.xAuth = response.x_auth || appState.xAuth;
      renderModuleConfig();
      await refreshXOAuthStatus(true);
      if (appState.oauth.pendingAutoEnable && appState.xAuth && appState.xAuth.connected) {
        appendLog("system", "[SYSTEM] Platform status refreshed. Resuming auto generation.");
        toggleAutoGeneration(true);
      }
    } catch (errorRow) {
      appendApiError(errorRow);
      await refreshXOAuthStatus(true);
    } finally {
      clearOAuthAuthorizePending();
      clearOAuthCallbackQuery();
    }
  }

  async function connectXOAuth(options) {
    const opts = options || {};
    const fromAutoToggle = Boolean(opts.fromAutoToggle);
    try {
      appendLog("system", "[AUTH] Browser OAuth flow is disabled in runtime publish mode");
      appendLog("system", "[AUTH] Facebook token reissue must be handled through admin/manual setup flow");
      if (fromAutoToggle) {
        setActivity([
          "Runtime publish uses stored Facebook Page token only.",
          "No browser redirect will be opened.",
          "Run the admin/manual reissue flow if the token is expired.",
          "Refresh platform status after reissuing the token.",
        ]);
      }
      await refreshXOAuthStatus(true);
      renderModuleConfig();
    } catch (error) {
      appendApiError(error);
    }
  }

  function handleModalContinue() {
    const draft = savePreviewDraft();
    const context = appState.preview.context;
    hidePreviewModal();
    if (!draft || !context) return;

    if (context.source === "generation") {
      addDraftToPublishQueue(draft);
      return;
    }
    if (context.source === "publish-card") {
      const card = (appState.publishQueue || []).map(normalizePublishDraft).find((item) => item.draftId === context.cardId);
      if (!card) return;
      addDraftToPublishQueue({
        draft_id: context.cardId,
        platform: "facebook",
        category: draft.category || card.category,
        article_title: draft.article_title || card.articleTitle,
        source_link: draft.source_link || card.sourceLink,
        thumbnail_url: draft.thumbnail_url || card.thumbnailUrl,
        title: draft.title,
        body: draft.body,
        body_preview: draft.body.length > 220 ? `${draft.body.slice(0, 220)}...` : draft.body,
        cycle_id: draft.cycle_id || card.cycleId || "",
        summary: card.summary,
        generated_post: draft.body,
        relevance_score: card.relevanceScore,
        research_relevance_score: card.researchRelevanceScore,
        why_it_matters: card.whyItMatters,
        target_audience: card.targetAudience,
        post_angle: card.postAngle,
        post_summary: card.postSummary,
        risk_of_misleading: card.riskOfMisleading,
        final_recommendation: card.finalRecommendation,
        review_notes: card.reviewNotes,
        content_quality_score: card.contentQualityScore,
        article_relevant: card.articleRelevant,
        post_tone: card.postTone,
        approval_channel: "telegram",
        approval_status: "pending",
        operator_decision: "modify",
        operator_modified: true,
        approved_for_queue: false,
        resend_telegram_review: true,
      });
      appendLog("system", "[SYSTEM] Publish draft updated and re-sent to Telegram review");
    }
  }

  async function rejectDraftAndRetry(draftId, draftPayload, previewContext) {
    const draft = draftPayload || appState.preview.draft;
    const context = previewContext || appState.preview.context || {};
    const candidateDraftId = String(draftId || (draft && draft.draft_id) || "").trim();
    if (!draft || !candidateDraftId) return;

    const cycleId = String(draft.cycle_id || context.cycleId || appState.loop.currentCycleId || `cycle_${Date.now()}`);
    try {
      const response = await apiJson("/api/crew/social/draft/reject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          cycle_id: cycleId,
          draft_id: candidateDraftId,
          source_link: draft.source_link,
          article_title: draft.article_title,
          category: draft.category,
        }),
      });
      (response.logs || []).forEach(appendTagged);
      appState.loop.currentCycleId = cycleId;
      if (appState.generationEnabled) {
        updateGenerationState("generating");
        setActivity(["Draft rejected.", "Selecting another candidate in this cycle...", "Generating next draft..."]);
        const mode = appState.currentPostId ? "normal" : "bootstrap";
        await runGenerationCycle(mode, { cycleId });
      } else {
        updateGenerationState("stopped");
      }
    } catch (error) {
      updateGenerationState("error");
      appendLog("error", `[ERROR] ${String(error.message || error)}`);
    }
  }

  async function editDraft(draftId, newContent) {
    const did = String(draftId || "").trim();
    const body = String(newContent || "").trim();
    if (!did || !body) return false;
    const card = (appState.publishQueue || []).map(normalizePublishDraft).find((item) => item.draftId === did);
    if (!card) return false;
    await addDraftToPublishQueue({
      draft_id: did,
      platform: "facebook",
      category: card.category,
      article_title: card.articleTitle,
      source_link: card.sourceLink,
      thumbnail_url: card.thumbnailUrl,
      title: card.title,
      body,
      body_preview: body.length > 220 ? `${body.slice(0, 220)}...` : body,
      cycle_id: card.cycleId || "",
      summary: card.summary,
      generated_post: body,
      relevance_score: card.relevanceScore,
      research_relevance_score: card.researchRelevanceScore,
      why_it_matters: card.whyItMatters,
      target_audience: card.targetAudience,
      post_angle: card.postAngle,
      post_summary: card.postSummary,
      risk_of_misleading: card.riskOfMisleading,
      final_recommendation: card.finalRecommendation,
      review_notes: card.reviewNotes,
      content_quality_score: card.contentQualityScore,
      article_relevant: card.articleRelevant,
      post_tone: card.postTone,
      approved_for_queue: card.finalRecommendation === "publish",
    });
    appendLog("system", "[SYSTEM] Publish draft edited");
    return true;
  }

  function insertQuickCommand(actionName) {
    const command = QUICK_ACTION[actionName];
    if (!command || !dom.commandInput) return;
    const current = String(dom.commandInput.value || "").trim();
    const escaped = command.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const exists = new RegExp(`(^|\\s)${escaped}(?=\\s|$)`, "i").test(current);
    const merged = current ? (exists ? current : `${command} ${current}`) : command;
    dom.commandInput.value = merged;
    dom.commandInput.focus();
    dom.commandInput.setSelectionRange(merged.length, merged.length);
    if (dom.commandResponse) dom.commandResponse.textContent = `Prepared command: ${merged}`;
    appendLog("system", `[SYSTEM] Quick action inserted: ${merged}`);
  }

  async function runSocialCommand(command) {
    const text = String(command || "").trim().toLowerCase();
    if (text.startsWith("/content generate workconnect clips")) {
      await generateContentClip();
      if (dom.commandResponse) dom.commandResponse.textContent = "WorkConnect Clips generation started.";
      return true;
    }
    if (!text.startsWith("/social")) return false;
    if (text.startsWith("/social collect news")) {
      const response = await apiJson("/api/crew/social/collect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          news_source: appState.socialForm.newsSource,
          keyword_set: appState.socialForm.keywordSet,
        }),
      });
      (response.logs || []).forEach(appendTagged);
      if (dom.commandResponse) dom.commandResponse.textContent = `Collected ${response.filtered_count || 0} filtered news items.`;
      return true;
    }
    if (text.startsWith("/social review latest")) {
      const response = await apiJson("/api/crew/social/review-latest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          news_source: appState.socialForm.newsSource,
          keyword_set: appState.socialForm.keywordSet,
        }),
      });
      (response.logs || []).forEach(appendTagged);
      if (dom.commandResponse) dom.commandResponse.textContent = `Reviewed ${((response.reviewed_items || []).length)} latest articles.`;
      return true;
    }
    if (text.startsWith("/social generate facebook posts")) {
      await runGenerationCycle("manual");
      if (dom.commandResponse) dom.commandResponse.textContent = "Facebook generation pipeline started.";
      return true;
    }
    if (text.startsWith("/social consult queue")) {
      const response = await apiJson("/api/crew/social/consult-queue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      (response.logs || []).forEach(appendTagged);
      appState.publishQueue = response.publish_queue || [];
      renderPublishCards();
      if (dom.commandResponse) dom.commandResponse.textContent = `Consulted ${((response.updated || []).length)} queued candidates.`;
      return true;
    }
    if (text.startsWith("/social telegram test")) {
      const response = await apiJson("/api/crew/telegram/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      (response.logs || []).forEach(appendTagged);
      if (dom.commandResponse) dom.commandResponse.textContent = "Telegram test message sent.";
      return true;
    }
    if (text.startsWith("/social publish facebook")) {
      const firstApproved = (appState.publishQueue || [])
        .map(normalizePublishDraft)
        .find((item) => String(item.finalRecommendation || "").toLowerCase() === "publish");
      if (!firstApproved) {
        appendLog("error", "[ERROR] No publish-approved Facebook card is queued.");
        if (dom.commandResponse) dom.commandResponse.textContent = "No publish-approved Facebook card is queued.";
        return true;
      }
      await approvePublish(firstApproved.draftId);
      if (dom.commandResponse) dom.commandResponse.textContent = `Publishing queued Facebook card: ${firstApproved.draftId}`;
      return true;
    }
    return false;
  }

  function rerender() {
    renderModules();
    renderTaskQueue();
    renderFilterButtons();
    applyLogFilter();
    renderModuleConfig();
    renderPublishCards();
    if (newsCollectorController) newsCollectorController.renderScreen();
  }

  dom.moduleButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const moduleName = button.dataset.module;
      if (!moduleName) return;
      if (appState.selectedModule !== moduleName) {
        selectModule(moduleName);
      } else {
        toggleModuleActive(moduleName);
      }
    });
  });

  dom.quickActionButtons.forEach((button) => {
    button.addEventListener("click", () => insertQuickCommand(button.dataset.action || ""));
  });

  dom.filterButtons.forEach((button) => {
    button.addEventListener("click", () => {
      appState.logFilter = String(button.dataset.filter || "all").toLowerCase();
      applyLogFilter();
      persistUiButtonState();
    });
  });

  if (dom.commandForm && dom.commandInput) {
    dom.commandForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const command = String(dom.commandInput.value || "").trim();
      if (!command) return;
      appendLog("agent", `[AGENT] Command received: ${command}`);
      try {
        const handled = await runSocialCommand(command);
        if (!handled && dom.commandResponse) {
          dom.commandResponse.textContent = `Received command${appState.selectedModule ? ` (${appState.selectedModule})` : ""}: ${command}`;
        }
      } catch (error) {
        appendApiError(error);
      } finally {
        dom.commandInput.value = "";
        dom.commandInput.focus();
      }
    });
  }

  if (dom.modalEdit && dom.modalTitle && dom.modalBody) {
    dom.modalEdit.addEventListener("click", () => {
      if (!appState.preview.draft) return;
      appState.preview.editing = !appState.preview.editing;
      dom.modalTitle.readOnly = !appState.preview.editing;
      dom.modalBody.readOnly = !appState.preview.editing;
      dom.modalEdit.textContent = appState.preview.editing ? "Save" : "Edit";
      if (!appState.preview.editing) savePreviewDraft();
    });
  }

  if (dom.modalContinue) dom.modalContinue.addEventListener("click", handleModalContinue);
  if (dom.modalStop) {
    dom.modalStop.addEventListener("click", async () => {
      const draft = appState.preview.draft ? { ...appState.preview.draft } : null;
      const context = appState.preview.context ? { ...appState.preview.context } : null;
      hidePreviewModal();
      appendLog("preview", "[PREVIEW] Preview flow stopped by user");
      if (context && context.source === "generation" && draft && draft.draft_id) {
        await rejectDraftAndRetry(draft.draft_id, draft, context);
        return;
      }
      if (appState.generationEnabled) {
        updateGenerationState(appState.currentPostId ? "monitoring" : "bootstrap");
      } else {
        updateGenerationState("stopped");
      }
    });
  }
  if (dom.modal) {
    dom.modal.addEventListener("click", (event) => {
      // Keep preview modal open on outside click; close only via action buttons.
      if (event.target === dom.modal) {
        event.preventDefault();
      }
    });
  }

  document.addEventListener("click", handleDynamicClick);
  document.addEventListener("change", handleDynamicChange);
  document.addEventListener("input", handleDynamicInput);
  window.addEventListener("message", (event) => {
    const payload = event && event.data ? event.data : {};
    handleOAuthCallbackPayload(payload);
  });
  window.addEventListener("beforeunload", persistUiButtonState);

  window.toggleAutoGeneration = toggleAutoGeneration;
  window.startGenerationLoop = startGenerationLoopAlias;
  window.stopGenerationLoop = stopGenerationLoopAlias;
  window.runGenerationCycle = runGenerationCycle;
  window.updateGenerationState = updateGenerationState;
  window.registerPublishedPost = registerPublishedPost;
  window.resumeMonitoringAfterPublish = resumeMonitoringAfterPublish;
  window.monitorCurrentPost = monitorCurrentPost;
  window.shouldFetchLikers = shouldFetchLikers;
  window.fetchLikingUsers = fetchLikingUsers;
  window.dedupeSavedLikers = dedupeSavedLikers;
  window.saveNewLikers = saveNewLikers;
  window.beforeGenerateNextPost = beforeGenerateNextPost;
  window.closePostMonitor = closePostMonitor;
  window.showDraftPreviewModal = showPreviewModal;
  window.rejectDraftAndRetry = rejectDraftAndRetry;
  window.editDraft = editDraft;
  window.addDraftToPublishQueue = addDraftToPublishQueue;
  window.renderPublishQueue = renderPublishQueue;
  window.approvePublish = approvePublish;
  window.cancelPublish = cancelPublish;

  window.setInterval(() => {
    if (newsCollectorController && appState.selectedModule === "News Collector") {
      newsCollectorController.refreshIfSelected();
    }
    if (appState.selectedModule === "Content") {
      refreshContentQueue(true);
    }
  }, MONITOR_INTERVAL_MS);

  setActivity([
    "System ready.",
    "All modules inactive. Select a module.",
    "Auto generation loop is OFF.",
  ]);
  rerender();
  appendLog("system", `[SYSTEM] Module panel ready: ${moduleOrder.length} modules inactive`);
  clearOAuthAuthorizePending();
  try {
    localStorage.removeItem("crew_x_oauth_callback_result");
  } catch (error) {
    appendLog("error", `[ERROR] oauth callback cache clear failed: ${String(error.message || error)}`);
  }

  loadBootstrap()
    .then(() => maybeRestoreUiButtonState())
    .then(() => (newsCollectorController ? newsCollectorController.afterBootstrap() : null))
    .then(() => refreshPublishQueue(true))
    .then(() => refreshContentQueue(true))
    .then(() => refreshPendingApprovals(true))
    .then(() => rerender())
    .catch((error) => appendLog("error", `[ERROR] ${String(error.message || error)}`));
})();
