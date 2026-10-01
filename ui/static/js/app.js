/* trans-bench UI — shared behaviour (theme, navigation, toasts, dialogs, tabs, palette, motion helpers).
   Everything hangs off window.TB; pages add their own scripts after this file. */
(() => {
  'use strict';

  const root = document.documentElement;
  const reduceMotion = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const $ = (sel, el = document) => el.querySelector(sel);
  const $$ = (sel, el = document) => Array.from(el.querySelectorAll(sel));
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const icon = (name, cls = '') => `<i data-lucide="${name}" class="${cls}"></i>`;
  // replaces every remaining <i data-lucide> in the document with its SVG
  const icons = () => { if (window.lucide) window.lucide.createIcons({ icons: window.lucide.icons, attrs: { 'aria-hidden': 'true' } }); };

  // ── Theme ───────────────────────────────────────────────────────────────
  // pref: auto | light | dark. The inline script in <head> applies it before first paint.
  const themeListeners = [];
  function themePref() { try { return localStorage.getItem('tb-theme') || 'auto'; } catch { return 'auto'; } }
  function applyTheme(pref) {
    root.setAttribute('data-theme-pref', pref);
    if (pref === 'auto') root.removeAttribute('data-theme'); else root.setAttribute('data-theme', pref);
    themeListeners.forEach((fn) => fn(isDark()));
  }
  function isDark() {
    const t = root.getAttribute('data-theme');
    return t ? t === 'dark' : window.matchMedia('(prefers-color-scheme: dark)').matches;
  }
  function cycleTheme() {
    const order = ['auto', 'light', 'dark'];
    const next = order[(order.indexOf(themePref()) + 1) % order.length];
    try { localStorage.setItem('tb-theme', next); } catch { /* private mode */ }
    const swap = () => applyTheme(next);
    if (document.startViewTransition && !reduceMotion()) document.startViewTransition(swap); else swap();
    toast(`Theme: ${next === 'auto' ? 'match system' : next}`, 'info', 1600);
  }
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => themeListeners.forEach((fn) => fn(isDark())));
  const cssVar = (name) => getComputedStyle(root).getPropertyValue(name).trim();

  // ── Toasts ──────────────────────────────────────────────────────────────
  const TOAST_ICONS = { success: 'circle-check', error: 'circle-x', warn: 'triangle-alert', info: 'info' };
  function toast(msg, type = 'info', ms = 4200) {
    let box = $('#toasts');
    if (!box) { box = document.createElement('div'); box.id = 'toasts'; box.className = 'toasts'; document.body.appendChild(box); }
    const t = document.createElement('div');
    t.className = `toast t-${type}`;
    t.setAttribute('role', type === 'error' ? 'alert' : 'status');
    t.innerHTML = `${icon(TOAST_ICONS[type] || 'info')}<div class="toast-msg">${esc(msg)}</div><button class="toast-x" aria-label="Dismiss">${icon('x')}</button>`;
    box.appendChild(t);
    icons(t);
    let timer;
    const close = () => { clearTimeout(timer); t.classList.add('is-leaving'); setTimeout(() => t.remove(), 260); };
    t.querySelector('.toast-x').addEventListener('click', close);
    t.addEventListener('mouseenter', () => clearTimeout(timer));
    t.addEventListener('mouseleave', () => { timer = setTimeout(close, 1800); });
    timer = setTimeout(close, ms);
    while (box.children.length > 4) box.firstElementChild.remove();
    return close;
  }

  // ── Dialogs ─────────────────────────────────────────────────────────────
  function openDialog(d) { if (!d.open) { d.classList.remove('is-closing'); d.showModal(); } }
  function closeDialog(d, value) {
    if (!d.open || d.classList.contains('is-closing')) return;
    if (reduceMotion()) { d.close(value); return; }
    d.classList.add('is-closing');
    setTimeout(() => { d.classList.remove('is-closing'); d.close(value); }, 130);
  }
  document.addEventListener('click', (e) => {
    // click on the backdrop (the dialog element itself) closes it
    if (e.target instanceof HTMLDialogElement && e.target.open && !e.target.hasAttribute('data-sticky')) closeDialog(e.target, 'cancel');
    const closer = e.target.closest('[data-close-dialog]');
    if (closer) closeDialog(closer.closest('dialog'), closer.value || 'cancel');
    const opener = e.target.closest('[data-open-dialog]');
    if (opener) { const d = document.getElementById(opener.dataset.openDialog); if (d) openDialog(d); }
  });
  document.addEventListener('cancel', (e) => {
    if (e.target instanceof HTMLDialogElement) { e.preventDefault(); closeDialog(e.target, 'cancel'); }
  }, true);

  /** In-page replacement for window.confirm(). Resolves true/false. */
  function confirmDialog({ title = 'Are you sure?', body = '', confirmLabel = 'Confirm', cancelLabel = 'Cancel', danger = false, iconName } = {}) {
    return new Promise((resolve) => {
      const d = document.createElement('dialog');
      d.setAttribute('aria-labelledby', 'cfm-title');
      d.innerHTML = `<form method="dialog" class="dialog-card">
          <div class="dialog-body">
            <div class="dialog-icon ${danger ? 'danger' : ''}">${icon(iconName || (danger ? 'triangle-alert' : 'circle-help'))}</div>
            <h3 id="cfm-title">${esc(title)}</h3>${body ? `<p>${esc(body)}</p>` : ''}
          </div>
          <div class="dialog-foot">
            <button class="btn" value="cancel" data-close-dialog type="button">${esc(cancelLabel)}</button>
            <button class="btn ${danger ? 'btn-danger-solid' : 'btn-primary'}" value="ok" data-close-dialog type="button">${esc(confirmLabel)}</button>
          </div></form>`;
      document.body.appendChild(d);
      icons(d);
      d.addEventListener('close', () => { resolve(d.returnValue === 'ok'); d.remove(); });
      openDialog(d);
      d.querySelector('.btn-primary, .btn-danger-solid').focus();
    });
  }

  // ── Sidebar / drawer ────────────────────────────────────────────────────
  function setDrawer(open) {
    const sb = $('#sidebar');
    if (!sb) return;
    sb.classList.toggle('is-open', open);
    $$('.menu-btn').forEach((b) => b.setAttribute('aria-expanded', String(open)));
    if (open) $('.nav-link', sb)?.focus({ preventScroll: true });
  }
  function placeNavIndicator() {
    const nav = $('.nav');
    const ind = $('.nav-indicator');
    const cur = $('.nav-link[aria-current="page"]');
    if (!nav || !ind) return;
    if (!cur) { ind.style.opacity = '0'; return; }
    ind.style.opacity = '1';
    ind.style.height = `${cur.offsetHeight}px`;
    ind.style.transform = `translateY(${cur.offsetTop}px)`;
  }

  // ── Tabs ────────────────────────────────────────────────────────────────
  function initTabs(list) {
    const tabs = $$('[role="tab"]', list);
    let ink = $('.tab-ink', list);
    if (!ink) { ink = document.createElement('span'); ink.className = 'tab-ink'; list.appendChild(ink); }
    const moveInk = (t) => { ink.style.width = `${t.offsetWidth}px`; ink.style.transform = `translateX(${t.offsetLeft}px)`; };
    const select = (t, { focus = false, push = true } = {}) => {
      tabs.forEach((x) => {
        const on = x === t;
        x.setAttribute('aria-selected', String(on));
        x.tabIndex = on ? 0 : -1;
        const panel = document.getElementById(x.getAttribute('aria-controls'));
        if (panel) panel.hidden = !on;
      });
      moveInk(t);
      if (focus) t.focus();
      t.scrollIntoView({ block: 'nearest', inline: 'nearest' });
      if (push && list.hasAttribute('data-hash')) history.replaceState(null, '', `#${t.dataset.tab || t.id}`);
      list.dispatchEvent(new CustomEvent('tabchange', { detail: { tab: t, id: t.dataset.tab || t.id } }));
    };
    tabs.forEach((t, i) => {
      t.addEventListener('click', () => select(t));
      t.addEventListener('keydown', (e) => {
        const k = { ArrowRight: 1, ArrowLeft: -1 }[e.key];
        if (k) { e.preventDefault(); select(tabs[(i + k + tabs.length) % tabs.length], { focus: true }); }
        if (e.key === 'Home') { e.preventDefault(); select(tabs[0], { focus: true }); }
        if (e.key === 'End') { e.preventDefault(); select(tabs[tabs.length - 1], { focus: true }); }
      });
    });
    const fromHash = list.hasAttribute('data-hash') && tabs.find((t) => `#${t.dataset.tab || t.id}` === location.hash);
    select(fromHash || tabs.find((t) => t.getAttribute('aria-selected') === 'true') || tabs[0], { push: false });
    new ResizeObserver(() => { const cur = tabs.find((t) => t.getAttribute('aria-selected') === 'true'); if (cur) moveInk(cur); }).observe(list);
    list._select = (id) => { const t = tabs.find((x) => (x.dataset.tab || x.id) === id); if (t) select(t); };
    if (list.hasAttribute('data-hash')) {
      window.addEventListener('hashchange', () => { const t = tabs.find((x) => `#${x.dataset.tab || x.id}` === location.hash); if (t) select(t, { push: false }); });
    }
  }

  // ── Segmented controls (radio labels or aria-pressed buttons) ───────────
  function initSeg(seg) {
    let thumb = $('.seg-thumb', seg);
    if (!thumb) { thumb = document.createElement('span'); thumb.className = 'seg-thumb'; seg.prepend(thumb); }
    const place = () => {
      const on = $$('label, button', seg).find((x) => x.getAttribute('aria-pressed') === 'true' || x.querySelector('input:checked'));
      if (!on) { thumb.style.width = '0'; return; }
      thumb.style.width = `${on.offsetWidth}px`;
      thumb.style.transform = `translateX(${on.offsetLeft - 3}px)`;
    };
    seg.addEventListener('change', place);
    seg.addEventListener('click', (e) => {
      const b = e.target.closest('button');
      if (!b || !seg.contains(b)) return;
      $$('button', seg).forEach((x) => x.setAttribute('aria-pressed', String(x === b)));
      place();
      seg.dispatchEvent(new CustomEvent('segchange', { detail: { value: b.value } }));
    });
    new ResizeObserver(place).observe(seg);
    seg._place = place;
    place();
  }

  // ── Count-up numbers ────────────────────────────────────────────────────
  const fmt = new Intl.NumberFormat();
  function countUp(el) {
    const target = parseFloat(el.dataset.count);
    if (!isFinite(target)) return;
    const decimals = parseInt(el.dataset.decimals || '0', 10);
    const show = (v) => { el.textContent = decimals ? v.toFixed(decimals) : fmt.format(Math.round(v)); };
    if (reduceMotion()) { show(target); return; }
    const dur = Math.min(1400, 500 + Math.log10(Math.abs(target) + 1) * 220);
    const t0 = performance.now();
    const step = (now) => {
      const p = Math.min(1, (now - t0) / dur);
      show(target * (1 - Math.pow(1 - p, 4)));
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }

  // ── Reveal / stagger ────────────────────────────────────────────────────
  function stagger(scope = document) {
    $$('[data-stagger]', scope).forEach((group) => {
      Array.from(group.children).forEach((c, i) => { c.classList.add('reveal'); c.style.setProperty('--i', Math.min(i, 14)); });
    });
  }

  // ── Copy to clipboard ───────────────────────────────────────────────────
  async function copyText(text, btn) {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      const ta = Object.assign(document.createElement('textarea'), { value: text });
      document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove();
    }
    if (btn) {
      const prev = btn.innerHTML;
      btn.innerHTML = icon('check');
      icons(btn);
      setTimeout(() => { btn.innerHTML = prev; }, 1300);
    }
    toast('Copied to clipboard', 'success', 1600);
  }
  document.addEventListener('click', (e) => {
    const b = e.target.closest('[data-copy]');
    if (!b) return;
    const v = b.dataset.copy;
    const target = v.startsWith('#') ? document.querySelector(v) : null;
    copyText(target ? target.innerText.trim() : v, b);
  });

  // ── Sortable tables ─────────────────────────────────────────────────────
  function initSortable(table) {
    $$('th.sortable', table).forEach((th, idx) => {
      th.tabIndex = 0;
      const col = th.cellIndex;
      const sort = () => {
        const dir = th.getAttribute('aria-sort') === 'ascending' ? 'descending' : 'ascending';
        $$('th', table).forEach((x) => x.removeAttribute('aria-sort'));
        th.setAttribute('aria-sort', dir);
        const body = table.tBodies[0];
        const rows = Array.from(body.rows);
        const key = (r) => { const c = r.cells[col]; const v = c?.dataset.sort ?? c?.textContent.trim() ?? ''; const n = parseFloat(v); return isNaN(n) || th.dataset.type === 'text' ? v.toLowerCase() : n; };
        rows.sort((a, b) => { const x = key(a), y = key(b); return (x < y ? -1 : x > y ? 1 : 0) * (dir === 'ascending' ? 1 : -1); });
        rows.forEach((r) => body.appendChild(r));
      };
      th.addEventListener('click', sort);
      th.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); sort(); } });
      void idx;
    });
  }

  // ── Tooltip (one floating element shared by charts/matrices) ────────────
  const tip = {
    el: null,
    show(html, x, y) {
      if (!this.el) { this.el = document.createElement('div'); this.el.className = 'tip'; this.el.setAttribute('role', 'tooltip'); document.body.appendChild(this.el); }
      this.el.innerHTML = html;
      this.el.classList.add('on');
      const r = this.el.getBoundingClientRect();
      const px = Math.min(window.innerWidth - r.width - 8, Math.max(8, x + 14));
      const py = y + r.height + 24 > window.innerHeight ? y - r.height - 12 : y + 16;
      this.el.style.transform = `translate(${px}px, ${py}px)`;
    },
    hide() { this.el?.classList.remove('on'); },
  };

  // ── Experiment running chip (top bar) ───────────────────────────────────
  function pollRunning() {
    const slot = $('#run-slot');
    if (!slot || location.pathname === '/experiment/live') return;
    const tick = async () => {
      try {
        const r = await fetch('/experiment/status', { cache: 'no-store' });
        const s = await r.json();
        const p = s.progress || {};
        if (s.running) {
          const pct = p.total ? Math.round(p.pct ?? (100 * (p.done || 0)) / p.total) : null;
          slot.innerHTML = `<a class="run-chip" href="/experiment/live"><span class="live-dot"></span>Running${pct !== null ? ` · ${pct}%` : ''}</a>`;
        } else {
          slot.innerHTML = '';
        }
      } catch { /* server restarting */ }
    };
    tick();
    setInterval(tick, 5000);
  }

  // ── Command palette ─────────────────────────────────────────────────────
  const palette = {
    d: null, items: [], filtered: [], sel: 0, loaded: false,
    base: [
      { group: 'Go to', label: 'Dashboard', href: '/', icon: 'layout-dashboard', hint: 'g d' },
      { group: 'Go to', label: 'Systems', href: '/systems', icon: 'database', hint: 'g s' },
      { group: 'Go to', label: 'Topologies', href: '/graphs', icon: 'share-2', hint: 'g t' },
      { group: 'Go to', label: 'Campaigns', href: '/campaigns', icon: 'flask-conical', hint: 'g c' },
      { group: 'Go to', label: 'Results explorer', href: '/results', icon: 'chart-line', hint: 'g r' },
      { group: 'Go to', label: 'Live monitor', href: '/experiment/live', icon: 'activity', hint: 'g l' },
      { group: 'Actions', label: 'New experiment', href: '/experiment/new', icon: 'play', hint: 'n' },
      { group: 'Actions', label: 'Register a system', href: '/systems/new', icon: 'plus' },
      { group: 'Actions', label: 'Add a graph topology', href: '/graphs/new', icon: 'git-branch-plus' },
      { group: 'Actions', label: 'Add a domain', href: '/domains/new', icon: 'boxes' },
      { group: 'Actions', label: 'Toggle theme', run: () => cycleTheme(), icon: 'sun-moon', hint: 't' },
    ],
    build() {
      const d = document.createElement('dialog');
      d.className = 'cmdk-dialog';
      d.setAttribute('aria-label', 'Command palette');
      d.innerHTML = `<div class="dialog-card cmdk">
        <div class="cmdk-input">${icon('search')}<input type="text" placeholder="Jump to a page, system, topology or campaign…" aria-label="Search" role="combobox" aria-expanded="true" aria-controls="cmdk-list" autocomplete="off" spellcheck="false"><kbd>esc</kbd></div>
        <div class="cmdk-list" id="cmdk-list" role="listbox"></div>
        <div class="cmdk-foot"><span><kbd>↑</kbd><kbd>↓</kbd> move</span><span><kbd>↵</kbd> open</span><span><kbd>esc</kbd> close</span></div>
      </div>`;
      document.body.appendChild(d);
      icons(d);
      this.d = d;
      const input = $('input', d);
      input.addEventListener('input', () => this.filter(input.value));
      input.addEventListener('keydown', (e) => {
        if (e.key === 'ArrowDown') { e.preventDefault(); this.move(1); }
        if (e.key === 'ArrowUp') { e.preventDefault(); this.move(-1); }
        if (e.key === 'Enter') { e.preventDefault(); this.go(this.filtered[this.sel]); }
      });
      $('.cmdk-list', d).addEventListener('click', (e) => { const it = e.target.closest('.cmdk-item'); if (it) this.go(this.filtered[+it.dataset.i]); });
      $('.cmdk-list', d).addEventListener('mousemove', (e) => { const it = e.target.closest('.cmdk-item'); if (it && +it.dataset.i !== this.sel) { this.sel = +it.dataset.i; this.paint(false); } });
    },
    async load() {
      if (this.loaded) return;
      this.loaded = true;
      try {
        const r = await fetch('/api/nav');
        const n = await r.json();
        this.items = this.items.concat(
          n.systems.map((s) => ({ group: 'Systems', label: s.label, sub: s.name, href: `/systems/${s.name}`, icon: 'database' })),
          n.graphs.map((g) => ({ group: 'Topologies', label: g.label, sub: g.name, href: `/graphs/${g.name}`, icon: 'share-2' })),
          n.campaigns.map((c) => ({ group: 'Campaigns', label: c, href: `/campaigns/${c}`, icon: 'flask-conical' })),
        );
        if (this.d?.open) this.filter($('input', this.d).value);
      } catch { /* offline: static entries only */ }
    },
    open() {
      if (!this.d) { this.build(); this.items = this.base.slice(); }
      this.load();
      const input = $('input', this.d);
      input.value = '';
      this.filter('');
      openDialog(this.d);
      input.focus();
    },
    score(item, q) {
      if (!q) return 1;
      const hay = `${item.label} ${item.sub || ''}`.toLowerCase();
      const i = hay.indexOf(q);
      if (i === 0) return 4;
      if (i > 0) return hay[i - 1] === ' ' || hay[i - 1] === '_' ? 3 : 2;
      // every word of the query starts a word of the item ("new exp", "rev bin")
      const words = hay.split(/[\s_-]+/);
      return q.split(/\s+/).every((w) => words.some((x) => x.startsWith(w))) ? 1 : 0;
    },
    filter(q) {
      q = q.trim().toLowerCase();
      const order = ['Go to', 'Actions', 'Campaigns', 'Systems', 'Topologies'];
      const hits = this.items.map((it) => [it, this.score(it, q)]).filter(([, sc]) => sc > 0);
      // groups ordered by their best match, items by score inside a group
      const best = {};
      hits.forEach(([it, sc]) => { best[it.group] = Math.max(best[it.group] || 0, sc); });
      hits.sort((a, b) => (best[b[0].group] - best[a[0].group]) || (order.indexOf(a[0].group) - order.indexOf(b[0].group)) || (b[1] - a[1]));
      this.filtered = hits.map(([it]) => it).slice(0, 60);
      this.sel = 0;
      this.q = q;
      this.paint(true);
    },
    paint(rebuild) {
      const list = $('.cmdk-list', this.d);
      if (rebuild) {
        if (!this.filtered.length) { list.innerHTML = '<div class="cmdk-empty">No matches</div>'; return; }
        let html = ''; let group = '';
        const hl = (s) => { const t = esc(s); if (!this.q) return t; const i = s.toLowerCase().indexOf(this.q); return i < 0 ? t : `${esc(s.slice(0, i))}<mark>${esc(s.slice(i, i + this.q.length))}</mark>${esc(s.slice(i + this.q.length))}`; };
        this.filtered.forEach((it, i) => {
          if (it.group !== group) { group = it.group; html += `<div class="cmdk-group">${esc(group)}</div>`; }
          html += `<div class="cmdk-item" role="option" id="cmdk-${i}" data-i="${i}">${icon(it.icon)}<span>${hl(it.label)}</span>${it.sub && it.sub !== it.label ? `<span class="hint mono">${esc(it.sub)}</span>` : it.hint ? `<span class="hint"><kbd>${esc(it.hint)}</kbd></span>` : ''}</div>`;
        });
        list.innerHTML = html;
        icons(list);
      }
      $$('.cmdk-item', list).forEach((el) => el.setAttribute('aria-selected', String(+el.dataset.i === this.sel)));
      const cur = $(`#cmdk-${this.sel}`, list);
      cur?.scrollIntoView({ block: 'nearest' });
      $('input', this.d).setAttribute('aria-activedescendant', cur ? cur.id : '');
    },
    move(k) { if (!this.filtered.length) return; this.sel = (this.sel + k + this.filtered.length) % this.filtered.length; this.paint(false); },
    go(it) {
      if (!it) return;
      closeDialog(this.d);
      if (it.run) it.run(); else location.href = it.href;
    },
  };

  // ── Keyboard shortcuts ──────────────────────────────────────────────────
  let gPending = 0;
  document.addEventListener('keydown', (e) => {
    const typing = e.target.closest('input, textarea, select, [contenteditable], .CodeMirror');
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); palette.open(); return; }
    if (e.key === 'Escape' && $('#sidebar.is-open')) { setDrawer(false); return; }
    if (typing || e.metaKey || e.ctrlKey || e.altKey || document.querySelector('dialog[open]')) return;
    if (e.key === '/') { e.preventDefault(); palette.open(); return; }
    if (gPending && Date.now() - gPending < 900) {
      const to = { d: '/', s: '/systems', t: '/graphs', c: '/campaigns', r: '/results', l: '/experiment/live' }[e.key];
      gPending = 0;
      if (to) location.href = to;
      return;
    }
    if (e.key === 'g') { gPending = Date.now(); return; }
    if (e.key === 'n') location.href = '/experiment/new';
    if (e.key === 't') cycleTheme();
    if (e.key === '?') palette.open();
  });

  // ── System identity marks (the paper's marker shapes; same as MARKERS in _macros.html) ──
  const MARKERS = {
    x: '<path d="M3.5 3.5l9 9M12.5 3.5l-9 9" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"/>',
    s: '<rect x="3" y="3" width="10" height="10" rx="1"/>', v: '<path d="M2.5 4h11L8 13.5z"/>', '^': '<path d="M8 2.5L13.5 12h-11z"/>',
    '<': '<path d="M2.5 8L12.5 2.5v11z"/>', '>': '<path d="M13.5 8L3.5 2.5v11z"/>', D: '<path d="M8 1.8L14.2 8 8 14.2 1.8 8z"/>',
    o: '<circle cx="8" cy="8" r="5.5"/>', P: '<path d="M6 2.5h4V6h3.5v4H10v3.5H6V10H2.5V6H6z"/>',
    '*': '<path d="M8 1.5l1.9 4.3 4.6.4-3.5 3 1.1 4.6L8 11.4 3.9 13.8 5 9.2l-3.5-3 4.6-.4z"/>',
    p: '<path d="M8 2l5.7 4.1-2.2 6.7h-7L2.3 6.1z"/>', h: '<path d="M8 1.8l5.4 3.1v6.2L8 14.2l-5.4-3.1V4.9z"/>',
  };
  const marker = (style, cls = 'sys-mark') => `<span class="${cls}" ${style && style.color ? `style="--mark:${style.color}"` : ''} aria-hidden="true"><svg viewBox="0 0 16 16" fill="currentColor">${MARKERS[style ? style.marker : 'o'] || MARKERS.o}</svg></span>`;

  // ── Graph drawing (same geometry as the graph_svg macro in _macros.html) ─
  const ROUND = ['complete', 'max_acyclic', 'cycle', 'cycle_with_shortcuts', 'star', 'grid', 'barabasi_albert', 'scale_free'];
  function graphSvg(p, { closure = false, animate = true } = {}) {
    if (!p) return '<svg class="gdiag" viewBox="0 0 100 58"><text x="50" y="32" text-anchor="middle" font-size="7" fill="var(--ink-3)">no preview</text></svg>';
    const W = 100; const H = ROUND.includes(p.name) ? 100 : 58;
    const k = p.nodes.length;
    const r = k <= 10 ? 4.2 : k <= 16 ? 3.4 : k <= 30 ? 2.7 : 2.1;
    const arrows = p.edges.length <= 48;
    const pos = {};
    p.nodes.forEach((v) => { pos[v.id] = [v.x * W, v.y * H]; });
    const seg = (a, b, tail, cls, i, curve) => {
      const dx = b[0] - a[0]; const dy = b[1] - a[1]; const d = Math.hypot(dx, dy) || 1;
      const s = (r + 0.6) / d; const t = (r + tail) / d;
      const x1 = a[0] + dx * s; const y1 = a[1] + dy * s; const x2 = b[0] - dx * t; const y2 = b[1] - dy * t;
      if (!curve) return `<line class="${cls}" x1="${x1.toFixed(2)}" y1="${y1.toFixed(2)}" x2="${x2.toFixed(2)}" y2="${y2.toFixed(2)}" pathLength="1"${arrows ? ' marker-end="url(#tb-arrow)"' : ''} style="--i:${i}"/>`;
      // closure pairs bow outwards so they do not hide the edges underneath
      const mx = (x1 + x2) / 2 - (dy / d) * d * 0.18; const my = (y1 + y2) / 2 + (dx / d) * d * 0.18;
      return `<path class="${cls}" d="M${x1.toFixed(2)} ${y1.toFixed(2)}Q${mx.toFixed(2)} ${my.toFixed(2)} ${x2.toFixed(2)} ${y2.toFixed(2)}" pathLength="1" style="--i:${i}"/>`;
    };
    let out = `<svg class="gdiag${animate ? ' animate' : ''}" viewBox="-9 -9 ${W + 18} ${H + 18}" role="img" aria-label="Example ${esc(p.name)} graph with ${p.node_count} nodes and ${p.edge_count} edges">`;
    if (closure) out += (p.closure || []).map((e, i) => seg(pos[e.a], pos[e.b], 0.8, 'e tc', Math.min(i, 120), true)).join('');
    p.edges.forEach((e, i) => {
      const a = pos[e.a];
      if (e.self) out += `<circle class="e loop" cx="${a[0].toFixed(2)}" cy="${(a[1] - r * 1.9).toFixed(2)}" r="${(r * 1.1).toFixed(2)}" pathLength="1" style="--i:${i}"/>`;
      else out += seg(a, pos[e.b], arrows ? 2.4 : 0.6, 'e', i, false);
    });
    p.nodes.forEach((v, i) => { out += `<circle class="n" cx="${pos[v.id][0].toFixed(2)}" cy="${pos[v.id][1].toFixed(2)}" r="${r}" style="--i:${i}"><title>node ${v.id}</title></circle>`; });
    return `${out}</svg>`;
  }

  // ── Boot ────────────────────────────────────────────────────────────────
  function boot() {
    root.setAttribute('data-theme-pref', themePref());
    icons();
    stagger();
    placeNavIndicator();
    $$('[role="tablist"]').forEach(initTabs);
    $$('.seg').forEach(initSeg);
    $$('table[data-sortable]').forEach(initSortable);
    const io = 'IntersectionObserver' in window ? new IntersectionObserver((ents) => {
      ents.forEach((en) => { if (en.isIntersecting) { countUp(en.target); io.unobserve(en.target); } });
    }, { threshold: 0.4 }) : null;
    $$('[data-count]').forEach((el) => (io ? io.observe(el) : countUp(el)));
    $$('.menu-btn').forEach((b) => b.addEventListener('click', () => setDrawer(!$('#sidebar').classList.contains('is-open'))));
    $('.sidebar-scrim')?.addEventListener('click', () => setDrawer(false));
    $$('[data-theme-toggle]').forEach((b) => b.addEventListener('click', cycleTheme));
    $$('[data-palette]').forEach((b) => b.addEventListener('click', () => palette.open()));
    window.addEventListener('resize', placeNavIndicator);
    pollRunning();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();

  window.TB = {
    $, $$, esc, icon, icons, toast, confirm: confirmDialog, openDialog, closeDialog, copyText, countUp, stagger,
    initTabs, initSeg, initSortable, tip, isDark, cssVar, reduceMotion, graphSvg, marker,
    onTheme: (fn) => themeListeners.push(fn),
    fmt: (v, d = 2) => (v === null || v === undefined || Number.isNaN(v) ? '—' : Number(v).toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: 0 })),
    fmtSeconds: (s) => {
      if (s === null || s === undefined) return '—';
      if (s < 0.001) return `${(s * 1e6).toFixed(0)} µs`;
      if (s < 1) return `${(s * 1000).toFixed(s < 0.01 ? 2 : 1)} ms`;
      if (s < 60) return `${s.toFixed(s < 10 ? 2 : 1)} s`;
      const m = Math.floor(s / 60);
      return `${m}m ${Math.round(s - m * 60)}s`;
    },
    fmtBytes: (b) => {
      if (b === null || b === undefined) return '—';
      const u = ['B', 'KB', 'MB', 'GB', 'TB']; let i = 0;
      while (b >= 1024 && i < u.length - 1) { b /= 1024; i += 1; }
      return `${b.toFixed(b < 10 && i ? 1 : 0)} ${u[i]}`;
    },
    /** Chart.js defaults that follow the theme tokens. Call again after a theme change. */
    chartTheme() {
      if (!window.Chart) return;
      const C = window.Chart;
      C.defaults.font.family = cssVar('--font-body') || 'system-ui';
      C.defaults.font.size = 12;
      C.defaults.color = cssVar('--ink-2');
      C.defaults.borderColor = cssVar('--line');
      C.defaults.plugins.legend.labels.usePointStyle = true;
      C.defaults.plugins.legend.labels.boxWidth = 8;
      C.defaults.plugins.legend.labels.boxHeight = 8;
      C.defaults.plugins.tooltip.backgroundColor = cssVar('--ink');
      C.defaults.plugins.tooltip.titleColor = cssVar('--bg');
      C.defaults.plugins.tooltip.bodyColor = cssVar('--bg');
      C.defaults.plugins.tooltip.padding = 10;
      C.defaults.plugins.tooltip.cornerRadius = 8;
      C.defaults.plugins.tooltip.usePointStyle = true;
      C.defaults.animation.duration = reduceMotion() ? 0 : 700;
      C.defaults.animation.easing = 'easeOutQuart';
    },
    /** Series colour: plot-style colour, or the ink token for black (XSB). */
    seriesColor: (style) => (style && style.color) || cssVar('--ink'),
  };
  // backwards compatibility for inline handlers in older pages
  window.showToast = toast;
})();
