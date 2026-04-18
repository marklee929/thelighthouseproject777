// --- GEMINI V3 FIX ---
// the_light_house_project_777/web/static/script.js
document.addEventListener('DOMContentLoaded', () => {
  const appProjectRoot = String(window.__APP_PROJECT_ROOT__ || '').replace(/\\/g, '/').replace(/\/+$/, '');
  const appProjectName = String(window.__APP_PROJECT_NAME__ || 'the_light_house_project+777');
  const messageInput = document.getElementById('message-input');
  const sendButton = document.getElementById('send-button');
  const messagesContainer = document.querySelector('.messages');
  const codeContent = document.getElementById('code-content');
  const logStatus = document.getElementById('log-status');
  const pyqleEnabled = document.getElementById('pyqle-enabled');
  const pyqleMultiple = document.getElementById('pyqle-multiple');
  const pyqleReturnAll = document.getElementById('pyqle-return-all');
  const pyqleLoop = document.getElementById('pyqle-loop');
  const pyqleModeQuestion = document.getElementById('pyqle-mode-question');
  const pyqleModeCorrection = document.getElementById('pyqle-mode-correction');
  const pyqleModeChat = document.getElementById('pyqle-mode-chat');
  const pyqleModeLearn = document.getElementById('pyqle-mode-learn');
  const loopStartBtn = document.getElementById('pyqle-loop-start');
  const loopStopBtn = document.getElementById('pyqle-loop-stop');
  const medStartBtn = document.getElementById('meditation-start');
  const medStopBtn = document.getElementById('meditation-stop');
  codeContent.classList.add('language-plaintext');
  const centerLines = [];
  let lastTraceId = null;
  let pyqleAutoRestart = false;

  const traceDecision = document.getElementById('trace-decision');
  const traceFocus = document.getElementById('trace-focus');
  const traceScore = document.getElementById('trace-score');
  const traceTags = document.getElementById('trace-tags');
  const traceSignals = document.getElementById('trace-signals');
  const traceWb = document.getElementById('trace-wb');
  const traceEvidenceList = document.getElementById('trace-evidence-list');

  // --- Center Panel Buffer Logic ---
  const codePanel = document.querySelector('.panel.code-editor');
  const codeScrollEl = codePanel ? codePanel.querySelector('pre') : null;
  const centerPauseBtn = document.getElementById('center-pause');
  const centerCopy200Btn = document.getElementById('center-copy-200');
  const centerCopy1000Btn = document.getElementById('center-copy-1000');
  const centerControls = document.getElementById('center-controls');
  const centerIndicator = document.getElementById('center-indicator');
  const centerToast = document.getElementById('center-toast');
  const CENTER_MAX_BUFFER = 20000;
  let centerLiveMode = true;
  let centerRenderPaused = false;
  let centerControlsCollapsed = false;
  let centerToastTimer = null;
  setControlsCollapsed(false);

  function formatClock(ts) {
    if (!ts) return '--:--:--';
    return new Date(ts).toTimeString().slice(0, 8);
  }

  function applyCenterRender() {
    if (centerRenderPaused) return;
    if (!codeContent) return;
    codeContent.textContent = centerLines.map((item) => item.text).join('\n');
    if (centerLiveMode && codeScrollEl) {
      codeScrollEl.scrollTop = codeScrollEl.scrollHeight;
    }
  }

  function appendCenterLine(text) {
    if (!text) return;
    centerLines.push({ text, ts: Date.now() });
    if (centerLines.length > CENTER_MAX_BUFFER) {
      centerLines.splice(0, centerLines.length - CENTER_MAX_BUFFER);
    }
    applyCenterRender();
  }

  function showCenterToast(text) {
    if (!centerToast) return;
    if (centerToastTimer) clearTimeout(centerToastTimer);
    centerToast.textContent = text;
    centerToast.classList.add('show');
    centerToastTimer = setTimeout(() => {
      centerToast.classList.remove('show');
    }, 1600);
  }

  async function copyLastLines(count) {
    const slice = centerLines.slice(-count).map((item) => item.text).join('\n');
    if (!slice) {
      showCenterToast('Nothing to copy');
      return;
    }
    const bytes = new Blob([slice]).size;
    const approxKb = Math.max(1, Math.round(bytes / 1024));
    try {
      await navigator.clipboard.writeText(slice);
      showCenterToast(`Copied last ${count} lines (${approxKb} KB)`);
    } catch (err) {
      try {
        const ta = document.createElement('textarea');
        ta.value = slice;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        showCenterToast(`Copied last ${count} lines (${approxKb} KB)`);
      } catch (fallbackErr) {
        showCenterToast('Copy failed');
        console.error('copy failed', fallbackErr);
      }
    }
  }

  function setPauseState(paused) {
    centerRenderPaused = paused;
    if (centerPauseBtn) {
      centerPauseBtn.textContent = paused ? 'Resume' : 'Pause';
    }
    if (!paused) {
      applyCenterRender();
      if (codeScrollEl && centerLiveMode) {
        codeScrollEl.scrollTop = codeScrollEl.scrollHeight;
      }
    }
  }

  function setControlsCollapsed(collapsed) {
    centerControlsCollapsed = collapsed;
    if (centerControls) {
      centerControls.classList.toggle('collapsed', collapsed);
      centerControls.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    }
  }

  // Pause/Resume, Copy controls (render stops; buffer continues)
  if (centerPauseBtn) {
    centerPauseBtn.addEventListener('click', () => {
      setPauseState(!centerRenderPaused);
    });
  }
  if (centerCopy200Btn) {
    centerCopy200Btn.addEventListener('click', () => copyLastLines(200));
  }
  if (centerCopy1000Btn) {
    centerCopy1000Btn.addEventListener('click', () => copyLastLines(1000));
  }
  if (centerControls) {
    centerControls.addEventListener('click', (event) => {
      if (centerControlsCollapsed) {
        return;
      }
      if (event.target && event.target.closest('button')) {
        return;
      }
      setControlsCollapsed(true);
    });
  }
  if (centerIndicator) {
    centerIndicator.addEventListener('click', (event) => {
      event.stopPropagation();
      setControlsCollapsed(false);
    });
    centerIndicator.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        setControlsCollapsed(false);
      }
    });
  }

  // Scroll Handler for Center Panel (auto-scroll only)
  if (codeScrollEl) {
    codeScrollEl.addEventListener('scroll', () => {
      const atBottom = codeScrollEl.scrollHeight - codeScrollEl.scrollTop - codeScrollEl.clientHeight < 20; // 20px tolerance
      if (atBottom && !centerLiveMode) {
        centerLiveMode = true;
      } else if (!atBottom && centerLiveMode) {
        centerLiveMode = false;
      }
    });
  }

  const modeButtons = Array.from(document.querySelectorAll('.mode-btn'));
  let currentMode = 'chat';
  modeButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      modeButtons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentMode = btn.dataset.mode;
    });
  });

  async function fetchLatestTrace() {
    try {
      const res = await fetch('/api/turn/latest');
      const data = await res.json();
      if (!data.ok || !data.trace) return;
      const t = data.trace;
      lastTraceId = t.trace_id || t.traceId || null;
      traceDecision.textContent = t.decision || '-';
      const focus = t.focus || [];
      traceFocus.textContent = focus.length ? focus.join(', ') : '-';
      const ev = t.eval || {};
      traceScore.textContent = ev.score != null ? ev.score.toFixed(1) : '-';
      traceTags.textContent = (ev.tags || []).join(', ') || '-';
      const signals = ev.signals || {};
      const sigParts = [];
      if (signals.hub_ratio != null) sigParts.push(`hub ${signals.hub_ratio}`);
      if (signals.unique_ratio != null) sigParts.push(`uniq ${signals.unique_ratio}`);
      if (signals.tokens != null) sigParts.push(`tok ${signals.tokens}`);
      traceSignals.textContent = sigParts.join(' / ') || '-';
      traceWb.textContent = t.writeback_stats ? JSON.stringify(t.writeback_stats) : '-';
      traceEvidenceList.innerHTML = '';
      (t.evidence_selected_top || []).slice(0, 10).forEach(evItem => {
        const li = document.createElement('li');
        li.textContent = `${evItem.kw || ''} (${evItem.mem_id || evItem.mem || ''})`;
        traceEvidenceList.appendChild(li);
      });
    } catch (e) {
      console.error('trace fetch failed', e);
    }
  }

  const treeContainer = document.getElementById('file-tree');
  function renderTree(nodes, parentEl) {
    const ul = document.createElement('ul');
    nodes.forEach(n => {
      const li = document.createElement('li');
      li.textContent = n.name;
      li.classList.add(n.type);
      if (n.type === 'file') {
        li.addEventListener('click', async (e) => {
          e.stopPropagation();
          const p = await fetch(`/api/file?path=${encodeURIComponent(resolvePath(li))}`).then(r => r.json());
          if (p.ok) {
            codeContent.textContent = p.content || '';
            hljs.highlightElement(codeContent);
            codeContent.dataset.currentPath = p.path;
          } else {
            addMessage({ agent: 'System', message: TEXT.fileReadError(p.error) }, 'chat');
          }
        });
      } else if (n.type === 'directory' && n.children && n.children.length) {
        li.classList.add('collapsed');
        li.addEventListener('click', (e) => {
          e.stopPropagation();
          li.classList.toggle('collapsed');
        });
        li.appendChild(renderTree(n.children, li));
      }
      ul.appendChild(li);
    });
    return ul;
  }

  function resolvePath(liEl) {
    const parts = [];
    let cur = liEl;
    while (cur && cur !== treeContainer) {
      if (cur.tagName === 'LI') parts.unshift(cur.firstChild.textContent);
      cur = cur.parentElement;
    }
    return [appProjectRoot, ...parts].join('/').replace(/\/+/g, '/');
  }

  async function loadTree() {
    try {
      const res = await fetch('/api/files');
      const data = await res.json();
      treeContainer.innerHTML = '';
      treeContainer.appendChild(renderTree(data, treeContainer));
    } catch (e) {
      addMessage({ agent: 'System', message: TEXT.fileTreeError(e) }, 'chat');
    }
  }

  const TEXT = {
    fileReadError: (err) => `Failed to read file: ${err}`,
    fileTreeError: (err) => `Failed to load file tree: ${err}`,
    connected: 'Connected to the server successfully.',
    unknownType: (type) => `Unknown response type: ${type}`,
    nonJson: (data) => `Received a non-JSON message: ${data}`,
    wsError: 'A WebSocket error occurred. Reconnecting shortly.',
    connectionClosed: (code, reasonText) => `Connection to the server was closed. (code: ${code})${reasonText}`,
    reasonSuffix: (reason) => ` (reason: ${reason})`,
    fileCreateFail: 'Failed to create file.',
    created: (path) => `Created: ${path}`,
    selectForModify: 'Select a file to modify first.',
    fileModifyFail: 'Failed to modify file.',
    modified: (path) => `Modified: ${path}`,
    selectForDelete: 'Select a file to delete first.',
    fileDeleteFail: 'Failed to delete file.',
    deleted: (path) => `Deleted: ${path}`,
    reconnecting: 'Retrying the WebSocket connection. Please wait a moment.',
    processing: 'Processing request...',
    unknownCodeLabel: 'Unknown',
    uiCreatedHeader: '# Created from UI\n'
  };

  const wsScheme = location.protocol === 'https:' ? 'wss' : 'ws';
  const DEFAULT_WS_PORT = 8080;
  const forcedUrl = window.__PYQLE_WS_URL__;
  const computedHost = location.port ? location.host : `${location.hostname}:${DEFAULT_WS_PORT}`;
  const wsTargetHost = window.__PYQLE_WS_HOST__ || computedHost;
  const wsUrl = forcedUrl || `${wsScheme}://${wsTargetHost}/ws`;
  let ws = null;
  let reconnectTimer = null;
  let reconnectDelay = 1000;
  const MAX_RECONNECT_DELAY = 10000;
  let heartbeatTimer = null;
  let lastHeartbeatAck = Date.now();
  const HEARTBEAT_INTERVAL = 30000;
  const HEARTBEAT_TIMEOUT = 90000;

  function handleWsMessage(event) {
    try {
      const msg = JSON.parse(event.data);
      console.debug('WS IN:', msg);
      const data = msg.data ?? msg;

      switch (msg.type) {
        case 'agent_chat':
          addMessage({ agent: data.agent || 'Assistant', message: data.message || data.text || '' }, 'chat');
          appendCenterLine(`${data.agent || 'Assistant'}: ${data.message || data.text || ''}`);
          break;
        case 'log':
          appendCenterLine(`[${(data.event || 'log').toUpperCase()}] ${data.text || data.message || ''}`);
          break;
        case 'final_result':
          addMessage({ agent: 'Assistant', message: (typeof data === 'string' ? data : (data.message || data.text || '')) }, 'chat');
          appendCenterLine(`Final: ${typeof data === 'string' ? data : (data.message || data.text || '')}`);
          fetchLatestTrace();
          break;
        case 'pong':
          lastHeartbeatAck = Date.now();
          break;
        default:
          addMessage({ agent: 'System', message: TEXT.unknownType(msg.type) }, 'chat');
      }
    } catch (e) {
      addMessage({ agent: 'System', message: TEXT.nonJson(event.data) }, 'chat');
    }
  }

  function scheduleReconnect(immediate = false) {
    if (reconnectTimer) {
      return;
    }
    const delay = immediate ? 0 : reconnectDelay;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      reconnectDelay = Math.min(MAX_RECONNECT_DELAY, reconnectDelay * 1.5);
      connectWebSocket();
    }, delay);
  }

  function stopHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function startHeartbeat() {
    stopHeartbeat();
    lastHeartbeatAck = Date.now();

    heartbeatTimer = setInterval(() => {
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        stopHeartbeat();
        return;
      }

      if (Date.now() - lastHeartbeatAck > HEARTBEAT_TIMEOUT) {
        console.warn('[WS] heartbeat timeout, closing socket');
        try {
          console.log('[WS] closing socket');
          ws.close();
        } catch (err) {
          console.error('[WS] close failed after heartbeat timeout', err);
        }
        stopHeartbeat();
        return;
      }
      try {
        ws.send(JSON.stringify({ type: 'ping', ts: Date.now() }));
      } catch (err) {
        console.error('[WS] heartbeat send failed', err);
        stopHeartbeat();
        scheduleReconnect();
      }
    }, HEARTBEAT_INTERVAL);
  }

  function connectWebSocket() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    try {
      const stateLabel = ws ? ws.readyState : 'none';
      if (ws && ws.readyState === WebSocket.CLOSING) {
        console.log('[WS] closing socket');
        ws.close();
      }
      ws = new WebSocket(wsUrl);
    } catch (err) {
      console.error('[WS] failed to create socket', err);
      scheduleReconnect();
      return;
    }

    ws.addEventListener('open', () => {
      addMessage({ agent: 'System', message: TEXT.connected }, 'chat');
      reconnectDelay = 1000;
      startHeartbeat();

      if (pyqleAutoRestart) {
        const payload = { question: "/pyqle", mode: "chat", options: {}, targets: [] };
        ws.send(JSON.stringify(payload));
        addMessage({ agent: 'System', message: 'PyQLE loop auto-restart requested.' }, 'chat');
      }
    });

    ws.addEventListener('message', handleWsMessage);

    ws.addEventListener('error', (event) => {
      console.error('[WS] error', event);
      addMessage({ agent: 'System', message: TEXT.wsError }, 'chat');
    });

    ws.addEventListener('close', (event, a, b, c, d) => {
      console.log(event, a, b, c, d)
      stopHeartbeat();
      const code = typeof event?.code === 'number' ? event.code : TEXT.unknownCodeLabel;
      const reasonText = event?.reason ? TEXT.reasonSuffix(event.reason) : '';
      addMessage({ agent: 'System', message: TEXT.connectionClosed(code, reasonText) }, 'chat');
      scheduleReconnect();
    });
  }

  connectWebSocket();
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && (!ws || ws.readyState === WebSocket.CLOSED)) {
      scheduleReconnect(true);
    }
  });

  function addMessage(message, target) {
    logBufferPush(message);
  }

  function currentPyqleMode() {
    if (pyqleModeQuestion && pyqleModeQuestion.checked) return 'question';
    if (pyqleModeCorrection && pyqleModeCorrection.checked) return 'correction';
    if (pyqleModeChat && pyqleModeChat.checked) return 'chat';
    if (pyqleModeLearn && pyqleModeLearn.checked) return 'learn';
    return 'chat';
  }

  function setLoopButtons(active) {
    if (loopStartBtn) loopStartBtn.disabled = active;
    if (loopStopBtn) loopStopBtn.disabled = !active;
  }
  setLoopButtons(false); // default: stopped

  function setMeditationButtons(active) {
    if (medStartBtn) medStartBtn.disabled = active;
    if (medStopBtn) medStopBtn.disabled = !active;
  }
  setMeditationButtons(false);

  function sendLoopCommand(start = true) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      addMessage({ agent: 'System', message: TEXT.reconnecting }, 'chat');
      connectWebSocket();
      return;
    }
    const mode = currentPyqleMode();
    if (mode === 'chat') {
      alert('Loop mode cannot be started while chat mode is selected.');
      setLoopButtons(false);
      return;
    }
    const options = { llama: mode !== 'chat', search: false, multi: false, return_all: false };
    const payload = {
      question: '',
      mode,
      options,
      loop: start,
    };
    ws.send(JSON.stringify(payload));
    addMessage({ agent: 'System', message: start ? '[loop] start requested' : '[loop] stop requested' }, 'chat');
    setLoopButtons(start);
  }

  if (loopStartBtn) {
    loopStartBtn.addEventListener('click', () => sendLoopCommand(true));
  }
  if (loopStopBtn) {
    loopStopBtn.addEventListener('click', () => sendLoopCommand(false));
  }

  function sendMeditationCommand(start = true) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      addMessage({ agent: 'System', message: TEXT.reconnecting }, 'chat');
      connectWebSocket();
      return;
    }
    if (start) {
      const payload = { cmd: 'meditation_v3_start', max_steps: null, sleep_sec: null };
      ws.send(JSON.stringify(payload));
      addMessage({ agent: 'System', message: `[meditation v3] start requested (always-on mode)` }, 'chat');
      setMeditationButtons(true);
    } else {
      const payload = { cmd: 'meditation_v3_stop' };
      ws.send(JSON.stringify(payload));
      addMessage({ agent: 'System', message: '[meditation v3] stop requested' }, 'chat');
      setMeditationButtons(false);
    }
  }

  if (medStartBtn) {
    medStartBtn.addEventListener('click', () => sendMeditationCommand(true));
  }
  if (medStopBtn) {
    medStopBtn.addEventListener('click', () => sendMeditationCommand(false));
  }

  async function doFileIO(mode, promptText) {
    const current = codeContent.dataset.currentPath || '';
    if (mode === 'create') {
      const target = promptText.match(/create\s+(.+?\.[A-Za-z0-9]+)\s*:/i)?.[1] || `${Date.now()}_new.txt`;
      const abs = `project/${target}`;
      const payload = { path: abs, content: `${TEXT.uiCreatedHeader}${promptText}\n` };
      const res = await fetch('/api/file', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).then(r => r.json());
      if (!res.ok) throw new Error(res.error || TEXT.fileCreateFail);
      addMessage({ agent: 'System', message: TEXT.created(abs) }, 'chat');
      await loadTree();
    }
    if (mode === 'modify') {
      if (!current) throw new Error(TEXT.selectForModify);
      const res = await fetch('/api/file', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: current, content: promptText }) }).then(r => r.json());
      if (!res.ok) throw new Error(res.error || TEXT.fileModifyFail);
      addMessage({ agent: 'System', message: TEXT.modified(current) }, 'chat');
      await loadTree();
    }
    if (mode === 'delete') {
      const target = current;
      if (!target) throw new Error(TEXT.selectForDelete);
      const res = await fetch(`/api/file?path=${encodeURIComponent(target)}`, { method: 'DELETE' }).then(r => r.json());
      if (!res.ok) throw new Error(res.error || TEXT.fileDeleteFail);
      addMessage({ agent: 'System', message: TEXT.deleted(target) }, 'chat');
      codeContent.textContent = '';
      delete codeContent.dataset.currentPath;
      await loadTree();
    }
  }

  function buildPyqleCommand(userText) {
    if (!pyqleEnabled || !pyqleEnabled.checked) return null;
    const parts = ["/pyqle", "--chat"];
    if (pyqleMultiple && pyqleMultiple.checked) parts.push("--multiple");
    if (pyqleReturnAll && pyqleReturnAll.checked) parts.push("--return-all");
    const txt = (userText || "").trim();
    parts.push(`"${txt || "ping"}"`);
    return parts.join(" ");
  }

  function sendMessage() {
    const rawText = messageInput.value.trim();
    if (!rawText) return;

    if (!ws || ws.readyState !== WebSocket.OPEN) {
      addMessage({ agent: 'System', message: TEXT.reconnecting }, 'chat');
      connectWebSocket();
      return;
    }

    if (['create', 'modify', 'delete'].includes(currentMode)) {
      doFileIO(currentMode, rawText).catch(err => {
        addMessage({ agent: 'System', message: err.message }, 'chat');
      });
    }

    // Default: preserve the existing mode behavior (including v1).
    let mode = currentMode;
    const options = { llama: true, search: false, multi: false, return_all: false };
    let questionText = rawText;
    const loopOn = pyqleLoop && pyqleLoop.checked;

    // If the PyQle console is enabled, override mode and options from the console state.
    const pyqleOn = pyqleEnabled && pyqleEnabled.checked;
    const multipleOn = pyqleMultiple && pyqleMultiple.checked;
    const returnAllOn = pyqleReturnAll && pyqleReturnAll.checked;
    const isQuestionRadio = pyqleModeQuestion && pyqleModeQuestion.checked;
    const isCorrectionRadio = typeof pyqleModeCorrection !== "undefined" && pyqleModeCorrection && pyqleModeCorrection.checked;
    const isChatRadio = pyqleModeChat && pyqleModeChat.checked;

    if (pyqleOn) {
      if (isQuestionRadio) {
        // v1 trigger: let the server keep the existing v1/pyqle branch handling (mode=question).
        mode = "question";
        // Options are not used here, but keep the default shape for payload compatibility.
      } else if (isCorrectionRadio) {
        mode = "correction";
        options.llama = true;
        options.multi = false;
        options.return_all = false;
      } else {
        // pyqle mode(v2/v3)
        mode = "pyqle";
        options.multi = !!multipleOn;
        options.return_all = !!returnAllOn;
        // correction: llama ON / chat: llama OFF
        options.llama = !isChatRadio;
      }
    }

    const payload = { question: questionText, mode, options, targets: [] };
    if (mode === 'question' || mode === 'pyqle') {
      payload.loop = loopOn && !isChatRadio; // Ignore loop mode when chat mode is selected.
    }
    ws.send(JSON.stringify(payload));
    addMessage({ agent: 'You', message: `[${mode}] ${questionText}` }, 'chat');
    messageInput.value = '';
    codeContent.textContent = TEXT.processing;
    fetchLatestTrace();
  }


  // Replace the bottom submit handling with shared sendMessage behavior.
  sendButton.addEventListener('click', (e) => {
    e.preventDefault();
    sendMessage();
  });

  messageInput.addEventListener('keydown', (e) => {
    // Ignore Enter while an IME composition is in progress.
    if (e.isComposing || e.keyCode === 229) return;

    if (e.key === 'Enter') {
      e.preventDefault();   // Prevent form submit and default Enter behavior.
      sendMessage();
    }
  });

  loadTree();
  fetchLatestTrace();

  // Feedback buttons
  Array.from(document.querySelectorAll('.fb-btn')).forEach(btn => {
    btn.addEventListener('click', async () => {
      const fb = btn.dataset.feedback;
      if (!fb) return;
      if (!lastTraceId) {
        addMessage({ agent: 'System', message: 'No trace available yet.' }, 'chat');
        return;
      }
      try {
        const res = await fetch(`/api/turn/${encodeURIComponent(lastTraceId)}/feedback`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ feedback: fb })
        });
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || 'feedback failed');
        addMessage({ agent: 'System', message: `Feedback saved (${fb}), score=${data.score}` }, 'chat');
        fetchLatestTrace();
      } catch (e) {
        addMessage({ agent: 'System', message: `Feedback error: ${e.message}` }, 'chat');
      }
    });
  });

  // Action buttons -> prefill input
  Array.from(document.querySelectorAll('.action-btn')).forEach(btn => {
    btn.addEventListener('click', () => {
      const action = btn.dataset.action;
      const focusText = traceFocus.textContent && traceFocus.textContent !== '-' ? traceFocus.textContent.split(',')[0].trim() : '';
      let prompt = '';
      if (action === 'define') prompt = focusText ? `Define ${focusText} in one sentence` : 'Define the target keyword in one sentence';
      if (action === 'examples') prompt = focusText ? `Give 3 examples of ${focusText}` : 'Give 3 examples of the target keyword';
      if (action === 'compare') prompt = focusText ? `Compare ${focusText} vs ` : 'Write the two keywords you want to compare: A vs B';
      if (action === 'counter') prompt = focusText ? `Give 2 counter perspectives on ${focusText}` : 'Give 2 counter perspectives on the topic';
      if (prompt) {
        messageInput.value = prompt;
        messageInput.focus();
      }
    });
  });
});

// --------- LOG BUFFER / LIVE VIEW ---------
(function () {
  const container = document.querySelector('.messages');
  const statusEl = document.getElementById('log-status');
  const LOG_SOFT_LIMIT = 500;
  const LOG_MAX_BUFFER = 2000;
  const VIRTUAL_WINDOW = 400;
  let logBuffer = [];
  let liveMode = true;

  function updateStatus() {
    if (!statusEl) return;
    statusEl.textContent = liveMode ? 'LIVE' : 'LIVE paused (scroll to bottom to resume)';
  }

  function renderLogs(forceBottom = false) {
    if (!container) return;
    const total = logBuffer.length;
    let start = 0;
    if (liveMode) {
      start = Math.max(0, total - VIRTUAL_WINDOW);
    } else {
      // keep a reasonable window even when paused
      start = Math.max(0, total - Math.max(LOG_MAX_BUFFER, VIRTUAL_WINDOW));
    }
    const slice = logBuffer.slice(start, total);
    container.innerHTML = '';
    const frag = document.createDocumentFragment();
    for (const m of slice) {
      const el = document.createElement('div');
      el.classList.add('message');
      const who = m.agent ?? 'Assistant';
      let txt = m.message ?? '';
      if ((who || '').toLowerCase() === 'pyqle' && txt.startsWith('PyQLE:')) {
        txt = txt.replace(/^PyQLE:\s*/, '');
      }
      el.innerHTML = `<strong>${who}:</strong> ${txt}`;
      frag.appendChild(el);
    }
    container.appendChild(frag);
    if (liveMode || forceBottom) {
      container.scrollTop = container.scrollHeight;
    }
  }

  function logBufferPush(message) {
    logBuffer.push(message);
    if (liveMode && logBuffer.length > LOG_MAX_BUFFER) {
      // trim to soft limit when exceeding max
      logBuffer = logBuffer.slice(logBuffer.length - LOG_SOFT_LIMIT);
    }
    renderLogs(true);
  }

  function handleScroll() {
    if (!container) return;
    const atBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 4;
    if (atBottom && !liveMode) {
      liveMode = true;
      // On resume, trim if needed
      if (logBuffer.length > LOG_SOFT_LIMIT) {
        logBuffer = logBuffer.slice(logBuffer.length - LOG_SOFT_LIMIT);
      }
      updateStatus();
      renderLogs(true);
    } else if (!atBottom && liveMode) {
      liveMode = false;
      updateStatus();
    }
  }

  if (container) {
    container.addEventListener('scroll', handleScroll);
  }
  updateStatus();
})();
