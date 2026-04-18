"use strict";

(function newsCollectorBootstrap(global) {
  const MODULE_NAME = "News Collector";
  const DEFAULT_LIMIT = 24;

  function createDefaultState() {
    return {
      candidates: [],
      feeds: [],
      count: 0,
      connectedCount: 0,
      loading: false,
      loadingFeeds: false,
      collecting: false,
      loadedOnce: false,
      feedsLoadedOnce: false,
      selectedArticleId: "",
      selectedFeedId: "",
      collection: {
        recent_hours: 1,
        item_limit: 4,
      },
      feedForm: {
        source_name: "",
        feed_name: "",
        feed_url: "",
        site_url: "",
      },
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
    state.collection = {
      recent_hours: Math.max(1, Math.min(Number(state.collection && state.collection.recent_hours ? state.collection.recent_hours : 1), 24)),
      item_limit: Math.max(1, Math.min(Number(state.collection && state.collection.item_limit ? state.collection.item_limit : 4), 10)),
    };

    function syncModuleState() {
      const modules = appState && appState.modules ? appState.modules : {};
      const moduleState = modules[MODULE_NAME];
      if (!moduleState) return;
      const hasFeeds = Array.isArray(state.feeds) && state.feeds.length > 0;
      const hasCandidates = Array.isArray(state.candidates) && state.candidates.length > 0;
      if (state.collecting || state.loading || state.loadingFeeds) {
        moduleState.status = "running";
        moduleState.active = true;
        return;
      }
      if (hasCandidates || Number(state.connectedCount || 0) > 0 || hasFeeds) {
        moduleState.status = "complete";
        moduleState.active = true;
        return;
      }
      moduleState.status = "idle";
    }

    function selectedCandidate() {
      return (state.candidates || []).find((item) => item.article_id === state.selectedArticleId) || null;
    }

    function selectedFeed() {
      return (state.feeds || []).find((item) => item.rss_feed_id === state.selectedFeedId) || null;
    }

    function feedCollectionLabel(feed) {
      return feed && feed.enabled ? "Collection ON" : "Collection OFF";
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

    function ensureFeedSelection() {
      const feeds = Array.isArray(state.feeds) ? state.feeds : [];
      if (!feeds.length) {
        state.selectedFeedId = "";
        return;
      }
      const current = selectedFeed();
      if (current) return;
      state.selectedFeedId = String(feeds[0].rss_feed_id || "");
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
      rerender();
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
        rerender();
      }
    }

    async function loadFeeds(options) {
      const opts = options || {};
      const silent = Boolean(opts.silent);
      if (typeof apiJson !== "function") return;
      state.loadingFeeds = true;
      rerender();
      try {
        const response = await apiJson("/api/crew/news-collector/feeds");
        state.feeds = Array.isArray(response.items) ? response.items : [];
        state.connectedCount = Number(response.connected_count || 0);
        state.feedsLoadedOnce = true;
        ensureFeedSelection();
        if (!silent) {
          appendLog("system", `[SYSTEM] News Collector loaded ${state.feeds.length} RSS feeds`);
        }
      } catch (error) {
        if (!silent) appendApiError(error);
      } finally {
        state.loadingFeeds = false;
        rerender();
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
        drop: "/api/crew/news-collector/drop",
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
          drop: "dropped from the review queue",
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

    async function submitFeedForm() {
      if (typeof apiJson !== "function") return;
      const feedUrl = String(state.feedForm.feed_url || "").trim();
      if (!feedUrl) {
        global.alert("Enter an RSS URL before adding a feed.");
        return;
      }
      try {
        const response = await apiJson("/api/crew/news-collector/feeds/add", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(state.feedForm),
        });
        appendLog("system", `[SYSTEM] RSS feed added: ${response.feed ? response.feed.feed_name || response.feed.feed_code : feedUrl}`);
        state.feedForm.feed_url = "";
        state.feedForm.feed_name = "";
        state.feedForm.source_name = "";
        state.feedForm.site_url = "";
        await loadFeeds({ silent: true });
        rerender();
      } catch (error) {
        appendApiError(error);
      }
    }

    function requireSelectedFeed() {
      const feed = selectedFeed();
      if (!feed) {
        global.alert("Select an RSS feed first.");
        return null;
      }
      return feed;
    }

    async function deleteSelectedFeed() {
      const feed = requireSelectedFeed();
      if (!feed || typeof apiJson !== "function") return;
      try {
        await apiJson("/api/crew/news-collector/feeds/delete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rss_feed_id: feed.rss_feed_id }),
        });
        appendLog("system", `[SYSTEM] RSS feed removed: ${feed.feed_name || feed.feed_code}`);
        state.selectedFeedId = "";
        await loadFeeds({ silent: true });
        rerender();
      } catch (error) {
        appendApiError(error);
      }
    }

    async function setSelectedFeedConnection(enabled) {
      const feed = requireSelectedFeed();
      if (!feed || typeof apiJson !== "function") return;
      try {
        await apiJson("/api/crew/news-collector/feeds/connection", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rss_feed_id: feed.rss_feed_id, enabled }),
        });
        appendLog("system", `[SYSTEM] RSS feed ${enabled ? "activated" : "deactivated"} for collection: ${feed.feed_name || feed.feed_code}`);
        await loadFeeds({ silent: true });
        rerender();
      } catch (error) {
        appendApiError(error);
      }
    }

    async function collectLatestNews() {
      if (state.connectedCount < 1) {
        global.alert("Activate at least one RSS feed before starting collection.");
        return;
      }
      if (typeof apiJson !== "function") return;
      state.collecting = true;
      rerender();
      try {
        const response = await apiJson("/api/crew/news-collector/collect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(state.collection),
        });
        const totals = response.totals || {};
        appendLog(
          "system",
          `[SYSTEM] RSS collection finished: feeds=${Number(totals.feeds_processed || 0)} saved=${Number(
            totals.items_saved || 0
          )} duplicate=${Number(totals.items_duplicate || 0)} failed=${Number(totals.items_failed || 0)}`
        );
        if (response.analysis_error) {
          appendLog("system", `[SYSTEM] Analysis fallback used: ${response.analysis_error}`);
        }
        await Promise.all([loadFeeds({ silent: true }), loadCandidates({ silent: true })]);
        rerender();
      } catch (error) {
        appendApiError(error);
      } finally {
        state.collecting = false;
        rerender();
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
      if (candidate.popularity_proxy !== null && candidate.popularity_proxy !== undefined && candidate.popularity_proxy !== "") {
        badges.push(`<span class="news-collector-badge">Pop ${esc(candidate.popularity_proxy)}</span>`);
      }
      if (candidate.age_minutes !== null && candidate.age_minutes !== undefined && candidate.age_minutes !== "") {
        badges.push(`<span class="news-collector-badge">${esc(candidate.age_minutes)}m</span>`);
      }
      return `<article class="news-collector-card ${candidate.article_id === state.selectedArticleId ? "is-selected" : ""}">
  ${imageBlock}
  <div class="news-collector-card-header">
    <h3 class="news-collector-card-title">${esc(candidate.title || "Untitled article")}</h3>
    <div class="news-collector-card-meta">${esc(candidate.source_name || "-")} | ${esc(candidate.published_at || candidate.collected_at || "-")}</div>
  </div>
  <div class="news-collector-badges">${badges.join("")}</div>
  <p class="news-collector-card-summary">${esc(candidate.summary || "")}</p>
  ${candidate.why_selected ? `<p class="news-collector-why">Why selected: ${esc(candidate.why_selected)}</p>` : ""}
  <div class="news-collector-card-actions">
    <button type="button" class="crew-btn primary" data-news-collector-action="approve" data-article-id="${esc(candidate.article_id)}">Approve</button>
    <button type="button" class="crew-btn ghost" data-news-collector-action="modify" data-article-id="${esc(candidate.article_id)}">Modify</button>
    <button type="button" class="crew-btn danger" data-news-collector-action="reject" data-article-id="${esc(candidate.article_id)}">Reject</button>
    <button type="button" class="crew-btn ghost" data-news-collector-action="drop" data-article-id="${esc(candidate.article_id)}">Drop</button>
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
      syncModuleState();
      if (!dom.newsCollectorList || !dom.newsCollectorCount || !dom.newsCollectorEmpty) return;
      const active = appState.selectedModule === MODULE_NAME;
      if (!active) return;
      const count = Array.isArray(state.candidates) ? state.candidates.length : 0;
      dom.newsCollectorCount.textContent = state.loading
        ? "Loading..."
        : `${count} Candidates | ${Number(state.connectedCount || 0)} Active Feeds`;
      dom.newsCollectorEmpty.classList.toggle("is-hidden", count > 0);
      if (state.loading) {
        dom.newsCollectorEmpty.textContent = "Loading reviewable article candidates from PostgreSQL...";
      } else if (state.collecting) {
        dom.newsCollectorEmpty.textContent = "Collecting latest Christian RSS articles and scoring them for PLD-fit...";
      } else if (!count) {
        dom.newsCollectorEmpty.textContent = "No collected article candidates are ready for review.";
      }
      dom.newsCollectorList.innerHTML = count ? state.candidates.map(renderCard).join("") : "";
    }

    function renderModuleConfig() {
      if (!dom.moduleConfigContent || appState.selectedModule !== MODULE_NAME) return false;
      ensureSelection();
      ensureFeedSelection();
      const candidate = selectedCandidate();
      const selectedFeedRow = selectedFeed();
      const feeds = Array.isArray(state.feeds) ? state.feeds : [];
      const hasSelectedFeed = Boolean(selectedFeedRow);
      const feedListMarkup = feeds.length
        ? feeds
            .map(
              (feed) => `<button type="button" class="news-collector-feed-item ${
                feed.rss_feed_id === state.selectedFeedId ? "is-selected" : ""
              }" data-news-collector-action="select-feed" data-feed-id="${esc(feed.rss_feed_id)}">
  <span class="news-collector-feed-item-head">
    <span class="news-collector-feed-item-title">${esc(feed.feed_name || feed.feed_code || "Untitled feed")}</span>
    <span class="news-collector-feed-status ${feed.enabled ? "is-active" : "is-inactive"}">${feedCollectionLabel(feed)}</span>
  </span>
  <span class="news-collector-feed-item-meta">${esc(feed.source_name || "-")} | ${esc(
                feed.article_count
              )} articles</span>
</button>`
            )
            .join("")
        : `<div class="module-config-empty">No RSS feeds registered yet. Add one or use the seeded Christian feed registry.</div>`;
      const editorMarkup = candidate
        ? `<div class="news-collector-panel">
  <div class="news-collector-panel-header">
    <h3 class="news-collector-panel-title">${esc(candidate.title)}</h3>
    <a class="news-collector-panel-link" href="${esc(candidate.original_url)}" target="_blank" rel="noreferrer">Open original article</a>
  </div>
  <div class="config-group">
    <label class="config-label" for="news-collector-review-summary">Review Summary</label>
    <textarea id="news-collector-review-summary" class="config-textarea" rows="4" data-news-collector-field="review_summary">${esc(
      state.editor.review_summary || ""
    )}</textarea>
    <p class="config-help">This edits review-facing summary content only. Raw article truth stays unchanged.</p>
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
    <button type="button" class="crew-btn danger" data-news-collector-action="reject-editor" data-article-id="${esc(candidate.article_id)}">Reject</button>
    <button type="button" class="crew-btn ghost" data-news-collector-action="drop-editor" data-article-id="${esc(candidate.article_id)}">Drop</button>
  </div>
</div>`
        : `<div class="module-config-empty">Select a collected article card to edit the summary, angle, and question here.</div>`;

      dom.moduleConfigContent.innerHTML = `<div class="module-config-meta">
  Module: <span class="module-config-value">${esc(MODULE_NAME)}</span> |
  Active Feeds: <span class="module-config-value">${esc(state.connectedCount || 0)}</span>
</div>
<div class="news-collector-module-stack">
  <details class="news-collector-module-panel" open>
    <summary class="news-collector-module-summary">
      <span class="news-collector-module-summary-meta">
        <span class="news-collector-module-summary-title">Christian RSS Feed Manager</span>
        <span class="news-collector-module-summary-note">${state.loadingFeeds ? "Loading feeds..." : `${feeds.length} feeds | ${Number(
          state.connectedCount || 0
        )} active`}</span>
      </span>
    </summary>
    <div class="news-collector-feed-manager">
    <div class="config-group">
      <label class="config-label" for="news-collector-source-name">Source Name</label>
      <input id="news-collector-source-name" class="config-input" type="text" data-news-collector-feed-field="source_name" value="${esc(
        state.feedForm.source_name || ""
      )}" placeholder="Christian Post">
    </div>
    <div class="config-group">
      <label class="config-label" for="news-collector-feed-name">Feed Name</label>
      <input id="news-collector-feed-name" class="config-input" type="text" data-news-collector-feed-field="feed_name" value="${esc(
        state.feedForm.feed_name || ""
      )}" placeholder="Top stories RSS">
    </div>
    <div class="config-group">
      <label class="config-label" for="news-collector-feed-url">RSS URL</label>
      <input id="news-collector-feed-url" class="config-input" type="url" data-news-collector-feed-field="feed_url" value="${esc(
        state.feedForm.feed_url || ""
      )}" placeholder="https://example.com/rss">
    </div>
    <div class="config-group">
      <label class="config-label" for="news-collector-site-url">Site URL</label>
      <input id="news-collector-site-url" class="config-input" type="url" data-news-collector-feed-field="site_url" value="${esc(
        state.feedForm.site_url || ""
      )}" placeholder="https://example.com">
    </div>
    <div class="news-collector-feed-actions">
      <button type="button" class="crew-btn primary" data-news-collector-action="add-feed">Add Feed</button>
      <button type="button" class="crew-btn danger" data-news-collector-action="delete-feed" ${hasSelectedFeed ? "" : "disabled"}>Delete Selected</button>
      <button type="button" class="crew-btn ghost" data-news-collector-action="activate-feed" ${
        !hasSelectedFeed || selectedFeedRow.enabled ? "disabled" : ""
      }>Activate Selected</button>
      <button type="button" class="crew-btn ghost" data-news-collector-action="deactivate-feed" ${
        !hasSelectedFeed || !selectedFeedRow.enabled ? "disabled" : ""
      }>Deactivate Selected</button>
    </div>
    <p class="config-help"><code>Add Feed</code> saves the RSS entered above. <code>Delete Selected</code> removes the feed highlighted below. <code>Activate Selected</code> and <code>Deactivate Selected</code> control whether that RSS is used for actual news collection.</p>
    <div class="news-collector-feed-selection">
      ${
        hasSelectedFeed
          ? `Selected: <span class="module-config-value">${esc(selectedFeedRow.feed_name || selectedFeedRow.feed_code || "Untitled feed")}</span> | <span class="news-collector-feed-selection-status ${
              selectedFeedRow.enabled ? "is-active" : "is-inactive"
            }">${feedCollectionLabel(selectedFeedRow)}</span>`
          : `Selected: <span class="module-config-value">None</span> | Choose a feed below before deleting or changing collection status.`
      }
    </div>
    <div class="news-collector-feed-list">${feedListMarkup}</div>
    <div class="news-collector-collection-config">
      <div class="config-group">
        <label class="config-label" for="news-collector-recent-hours">Latest Window (hours)</label>
        <input id="news-collector-recent-hours" class="config-input" type="number" min="1" max="24" data-news-collector-collection-field="recent_hours" value="${esc(
          state.collection.recent_hours
        )}">
      </div>
      <div class="config-group">
        <label class="config-label" for="news-collector-item-limit">Items per Feed</label>
        <input id="news-collector-item-limit" class="config-input" type="number" min="1" max="10" data-news-collector-collection-field="item_limit" value="${esc(
          state.collection.item_limit
        )}">
      </div>
      <button type="button" class="crew-btn primary news-collector-collect-btn" data-news-collector-action="collect-latest" ${
        state.connectedCount < 1 || state.collecting ? "disabled" : ""
      }>${state.collecting ? "Collecting..." : "Start Collection"}</button>
    </div>
    <p class="config-help">Phase-1 default is to look back only 1 hour and keep the feed fetch small before PLD filtering narrows the final review set.</p>
    </div>
  </details>
  <details class="news-collector-module-panel" ${candidate ? "open" : ""}>
    <summary class="news-collector-module-summary">
      <span class="news-collector-module-summary-meta">
        <span class="news-collector-module-summary-title">Article Review Editor</span>
        <span class="news-collector-module-summary-note">${candidate ? esc(candidate.source_name || "-") : "No article selected"}</span>
      </span>
    </summary>
    <div class="news-collector-editor-wrap">
      ${editorMarkup}
    </div>
  </details>
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
        "Loading Christian RSS feed registry and collected candidates from PostgreSQL.",
        "Approve, modify, reject, or drop before Social generation.",
      ]);
      if (!state.feedsLoadedOnce && !state.loadingFeeds) {
        loadFeeds({ silent: true });
      }
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
      if (action === "select-feed") {
        state.selectedFeedId = String(actionTarget.dataset.feedId || "").trim();
        renderModuleConfig();
        return true;
      }
      if (action === "add-feed") {
        submitFeedForm();
        return true;
      }
      if (action === "delete-feed") {
        deleteSelectedFeed();
        return true;
      }
      if (action === "activate-feed") {
        setSelectedFeedConnection(true);
        return true;
      }
      if (action === "deactivate-feed") {
        setSelectedFeedConnection(false);
        return true;
      }
      if (action === "collect-latest") {
        collectLatestNews();
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
      if (action === "drop") {
        submitAction("drop", articleId);
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
      if (action === "drop-editor") {
        submitAction("drop", articleId);
        return true;
      }
      return false;
    }

    function handleDynamicInput(event) {
      const target = event && event.target;
      if (!(target instanceof HTMLElement)) return false;
      const field = String(target.dataset.newsCollectorField || "").trim();
      if (field && (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement)) {
        state.editor[field] = target.value;
        state.editor.articleId = state.selectedArticleId;
        return true;
      }
      const feedField = String(target.dataset.newsCollectorFeedField || "").trim();
      if (feedField && (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement)) {
        state.feedForm[feedField] = target.value;
        return true;
      }
      const collectionField = String(target.dataset.newsCollectorCollectionField || "").trim();
      if (collectionField && target instanceof HTMLInputElement) {
        state.collection[collectionField] = Number(target.value || 0);
        return true;
      }
      return false;
    }

    function afterBootstrap() {
      if (!state.feedsLoadedOnce && !state.loadingFeeds) {
        loadFeeds({ silent: true });
      }
      if (appState.selectedModule === MODULE_NAME && !state.loadedOnce && !state.loading) {
        loadCandidates({ silent: true });
      }
      renderScreen();
    }

    function refreshIfSelected() {
      if (appState.selectedModule === MODULE_NAME) {
        if (!state.loadingFeeds) loadFeeds({ silent: true });
        if (!state.loading) loadCandidates({ silent: true });
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
