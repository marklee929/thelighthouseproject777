"use strict";

(function newsCollectorBootstrap(global) {
  const MODULE_NAME = "News Collector";
  const DEFAULT_LIMIT = 24;

  function createDefaultState() {
    return {
      candidates: [],
      count: 0,
      loading: false,
      loadedOnce: false,
      selectedArticleId: "",
      editor: {
        articleId: "",
        review_summary: "",
        suggested_angle: "",
        suggested_question: "",
        operator_note: "",
      },
    };
  }

  function createController(context) {
    const root = context && context.root ? context.root : null;
    const appState = context && context.appState ? context.appState : {};
    const dom = context && context.dom ? context.dom : {};
    const helpers = context && context.helpers ? context.helpers : {};
    const apiJson = helpers.apiJson;
    const appendLog = helpers.appendLog || function noop() {};
    const appendApiError = helpers.appendApiError || function noop() {};
    const esc = helpers.esc || ((value) => String(value ?? ""));
    const setActivity = helpers.setActivity || function noop() {};
    const rerender = helpers.rerender || function noop() {};

    if (!appState.newsCollector) {
      appState.newsCollector = createDefaultState();
    }

    const state = appState.newsCollector;

    function selectedCandidate() {
      return (state.candidates || []).find((item) => item.article_id === state.selectedArticleId) || null;
    }

    function seedEditorFromCandidate(candidate) {
      const row = candidate || null;
      state.editor = {
        articleId: row ? String(row.article_id || "") : "",
        review_summary: row ? String(row.review_summary || row.summary || "") : "",
        suggested_angle: row ? String(row.suggested_angle || "") : "",
        suggested_question: row ? String(row.suggested_question || "") : "",
        operator_note: row ? String(row.operator_note || "") : "",
      };
    }

    function ensureSelection() {
      const candidates = Array.isArray(state.candidates) ? state.candidates : [];
      if (!candidates.length) {
        state.selectedArticleId = "";
        seedEditorFromCandidate(null);
        return;
      }
      const current = selectedCandidate();
      if (current) {
        if (state.editor.articleId !== current.article_id) seedEditorFromCandidate(current);
        return;
      }
      state.selectedArticleId = String(candidates[0].article_id || "");
      seedEditorFromCandidate(candidates[0]);
    }

    function chooseCandidate(articleId) {
      const candidateId = String(articleId || "").trim();
      const candidate = (state.candidates || []).find((item) => item.article_id === candidateId);
      if (!candidate) return;
      state.selectedArticleId = candidate.article_id;
      seedEditorFromCandidate(candidate);
      renderScreen();
      renderModuleConfig();
    }

    async function loadCandidates(options) {
      const opts = options || {};
      const silent = Boolean(opts.silent);
      const limit = Math.max(1, Math.min(Number(opts.limit || state.limit || DEFAULT_LIMIT), 100));
      if (typeof apiJson !== "function") return;
      state.loading = true;
      renderScreen();
      try {
        const response = await apiJson(`/api/crew/news-collector/candidates?limit=${limit}`);
        state.candidates = Array.isArray(response.items) ? response.items : [];
        state.count = Number(response.count || state.candidates.length);
        state.loadedOnce = true;
        ensureSelection();
        if (!silent) {
          appendLog("system", `[SYSTEM] News Collector loaded ${state.candidates.length} article candidates`);
        }
      } catch (error) {
        if (!silent) appendApiError(error);
      } finally {
        state.loading = false;
        renderScreen();
        renderModuleConfig();
      }
    }

    function payloadForArticle(articleId) {
      const selectedId = String(state.selectedArticleId || "");
      const usingEditor = selectedId && articleId === selectedId;
      return {
        article_id: articleId,
        review_summary: usingEditor ? String(state.editor.review_summary || "") : "",
        suggested_angle: usingEditor ? String(state.editor.suggested_angle || "") : "",
        suggested_question: usingEditor ? String(state.editor.suggested_question || "") : "",
        operator_note: usingEditor ? String(state.editor.operator_note || "") : "",
      };
    }

    async function submitAction(action, articleId) {
      const id = String(articleId || "").trim();
      if (!id || typeof apiJson !== "function") return;
      const endpointMap = {
        approve: "/api/crew/news-collector/approve",
        modify: "/api/crew/news-collector/modify",
        reject: "/api/crew/news-collector/reject",
      };
      const endpoint = endpointMap[action];
      if (!endpoint) return;
      try {
        const response = await apiJson(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payloadForArticle(id)),
        });
        const labelMap = {
          approve: "approved for Social candidate generation",
          modify: "saved as review-facing modification",
          reject: "rejected from candidate review",
        };
        appendLog("system", `[SYSTEM] Article ${id} ${labelMap[action] || action}`);
        if (response && response.queue_result && response.queue_result.generated_content_id) {
          appendLog("system", `[SYSTEM] Facebook candidate queued: ${response.queue_result.generated_content_id}`);
        }
        await loadCandidates({ silent: true });
        rerender();
      } catch (error) {
        appendApiError(error);
      }
    }

    function renderCard(candidate) {
      const imageBlock = candidate.image_url
        ? `<div class="news-collector-card-thumb-wrap"><img class="news-collector-card-thumb" src="${esc(
            candidate.image_url
          )}" alt="article thumbnail"></div>`
        : `<div class="news-collector-card-thumb-wrap"><div class="news-collector-card-thumb-placeholder">No image</div></div>`;
      const badges = [];
      if (candidate.final_score !== null && candidate.final_score !== undefined && candidate.final_score !== "") {
        badges.push(`<span class="news-collector-badge is-score">Score ${esc(candidate.final_score)}</span>`);
      }
      if (candidate.dominant_pld_stage) {
        badges.push(`<span class="news-collector-badge is-stage">${esc(candidate.dominant_pld_stage)}</span>`);
      }
      if (candidate.selection_status) {
        badges.push(`<span class="news-collector-badge">${esc(candidate.selection_status)}</span>`);
      }
      return `<article class="news-collector-card ${candidate.article_id === state.selectedArticleId ? "is-selected" : ""}">
  ${imageBlock}
  <div class="news-collector-card-header">
    <h3 class="news-collector-card-title">${esc(candidate.title || "Untitled article")}</h3>
    <div class="news-collector-card-meta">${esc(candidate.source_name || "-")} | ${esc(candidate.published_at || candidate.collected_at || "-")}</div>
  </div>
  <div class="news-collector-badges">${badges.join("")}</div>
  <p class="news-collector-card-summary">${esc(candidate.summary || "")}</p>
  ${
    candidate.why_selected
      ? `<p class="news-collector-why">Why selected: ${esc(candidate.why_selected)}</p>`
      : ""
  }
  <div class="news-collector-card-actions">
    <button type="button" class="crew-btn primary" data-news-collector-action="approve" data-article-id="${esc(candidate.article_id)}">Approve</button>
    <button type="button" class="crew-btn ghost" data-news-collector-action="modify" data-article-id="${esc(candidate.article_id)}">Modify</button>
    <button type="button" class="crew-btn danger" data-news-collector-action="reject" data-article-id="${esc(candidate.article_id)}">Reject</button>
    <button type="button" class="crew-btn ghost" data-news-collector-action="open-link" data-url="${esc(candidate.original_url)}" data-article-id="${esc(
        candidate.article_id
      )}">Open Link</button>
  </div>
</article>`;
    }

    function renderScreen() {
      if (root) {
        root.classList.toggle("show-news-collector", appState.selectedModule === MODULE_NAME);
      }
      if (!dom.newsCollectorList || !dom.newsCollectorCount || !dom.newsCollectorEmpty) return;
      const active = appState.selectedModule === MODULE_NAME;
      if (!active) return;
      const count = Array.isArray(state.candidates) ? state.candidates.length : 0;
      dom.newsCollectorCount.textContent = state.loading ? "Loading..." : `${count} Candidates`;
      dom.newsCollectorEmpty.classList.toggle("is-hidden", count > 0);
      if (state.loading) {
        dom.newsCollectorEmpty.textContent = "Loading reviewable article candidates from PostgreSQL...";
      } else if (!count) {
        dom.newsCollectorEmpty.textContent = "No collected article candidates are ready for review.";
      }
      dom.newsCollectorList.innerHTML = count ? state.candidates.map(renderCard).join("") : "";
    }

    function renderModuleConfig() {
      if (!dom.moduleConfigContent || appState.selectedModule !== MODULE_NAME) return false;
      ensureSelection();
      const candidate = selectedCandidate();
      if (!candidate) {
        dom.moduleConfigContent.innerHTML = `<div class="module-config-meta">Module: <span class="module-config-value">${esc(
          MODULE_NAME
        )}</span></div>
<div class="module-config-empty">Select News Collector and refresh when collected articles are ready.</div>`;
        return true;
      }

      dom.moduleConfigContent.innerHTML = `<div class="module-config-meta">
  Module: <span class="module-config-value">${esc(MODULE_NAME)}</span> |
  Source: <span class="module-config-value">${esc(candidate.source_name || "-")}</span>
</div>
<div class="news-collector-panel">
  <div class="news-collector-panel-header">
    <h3 class="news-collector-panel-title">${esc(candidate.title)}</h3>
    <a class="news-collector-panel-link" href="${esc(candidate.original_url)}" target="_blank" rel="noreferrer">Open original article</a>
  </div>
  <div class="config-group">
    <label class="config-label" for="news-collector-review-summary">Review Summary</label>
    <textarea id="news-collector-review-summary" class="config-textarea" rows="4" data-news-collector-field="review_summary">${esc(
      state.editor.review_summary || ""
    )}</textarea>
    <p class="config-help">This is a review-facing summary only. It does not overwrite the raw article truth.</p>
  </div>
  <div class="config-group">
    <label class="config-label" for="news-collector-suggested-angle">Suggested Angle</label>
    <input id="news-collector-suggested-angle" class="config-input" type="text" data-news-collector-field="suggested_angle" value="${esc(
      state.editor.suggested_angle || ""
    )}" placeholder="Curiosity-first angle">
  </div>
  <div class="config-group">
    <label class="config-label" for="news-collector-suggested-question">Suggested Question</label>
    <input id="news-collector-suggested-question" class="config-input" type="text" data-news-collector-field="suggested_question" value="${esc(
      state.editor.suggested_question || ""
    )}" placeholder="Reflection or entry question">
  </div>
  <div class="config-group">
    <label class="config-label" for="news-collector-operator-note">Operator Note</label>
    <textarea id="news-collector-operator-note" class="config-textarea" rows="4" data-news-collector-field="operator_note">${esc(
      state.editor.operator_note || ""
    )}</textarea>
  </div>
  <div class="news-collector-panel-actions">
    <button type="button" class="crew-btn primary" data-news-collector-action="approve-editor" data-article-id="${esc(candidate.article_id)}">Approve</button>
    <button type="button" class="crew-btn ghost" data-news-collector-action="save-modify" data-article-id="${esc(candidate.article_id)}">Save Modify</button>
    <button type="button" class="crew-btn danger wide" data-news-collector-action="reject-editor" data-article-id="${esc(candidate.article_id)}">Reject</button>
  </div>
</div>`;
      return true;
    }

    function handleModuleSelected(moduleName) {
      if (moduleName !== MODULE_NAME) {
        renderScreen();
        return;
      }
      setActivity([
        "News Collector module selected.",
        "Loading collected article candidates from PostgreSQL.",
        "Approve, modify, or reject before Social generation.",
      ]);
      if (!state.loadedOnce && !state.loading) {
        loadCandidates({ silent: true });
      } else {
        renderScreen();
        renderModuleConfig();
      }
    }

    function handleDynamicClick(event) {
      const target = event && event.target;
      if (!(target instanceof HTMLElement)) return false;
      const actionTarget = target.closest("[data-news-collector-action]");
      if (!(actionTarget instanceof HTMLElement)) return false;
      const action = String(actionTarget.dataset.newsCollectorAction || "").trim();
      if (!action) return false;
      const articleId = String(actionTarget.dataset.articleId || state.selectedArticleId || "").trim();
      if (action === "refresh") {
        loadCandidates({ silent: false });
        return true;
      }
      if (action === "modify") {
        chooseCandidate(articleId);
        appendLog("system", `[SYSTEM] News Collector editor opened for article ${articleId}`);
        return true;
      }
      if (action === "open-link") {
        const url = String(actionTarget.dataset.url || "").trim();
        if (url) {
          global.open(url, "_blank", "noopener,noreferrer");
        }
        return true;
      }
      if (action === "approve") {
        submitAction("approve", articleId);
        return true;
      }
      if (action === "reject") {
        submitAction("reject", articleId);
        return true;
      }
      if (action === "save-modify") {
        submitAction("modify", articleId);
        return true;
      }
      if (action === "approve-editor") {
        submitAction("approve", articleId);
        return true;
      }
      if (action === "reject-editor") {
        submitAction("reject", articleId);
        return true;
      }
      return false;
    }

    function handleDynamicInput(event) {
      const target = event && event.target;
      if (!(target instanceof HTMLElement)) return false;
      const field = String(target.dataset.newsCollectorField || "").trim();
      if (!field) return false;
      if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
        state.editor[field] = target.value;
        state.editor.articleId = state.selectedArticleId;
        return true;
      }
      return false;
    }

    function afterBootstrap() {
      if (appState.selectedModule === MODULE_NAME && !state.loadedOnce && !state.loading) {
        loadCandidates({ silent: true });
      }
      renderScreen();
    }

    function refreshIfSelected() {
      if (appState.selectedModule === MODULE_NAME && !state.loading) {
        loadCandidates({ silent: true });
      }
    }

    return {
      afterBootstrap,
      handleDynamicClick,
      handleDynamicInput,
      handleModuleSelected,
      refreshIfSelected,
      renderModuleConfig,
      renderScreen,
    };
  }

  global.CrewNewsCollector = {
    createController,
  };
})(window);
