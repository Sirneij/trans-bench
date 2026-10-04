/* Landing page motion graphics. Every drawing follows from data: the hero runs breadth-first waves
   (the iterations of a semi-naive evaluation from one start node) over a random graph, the scroll
   section evaluates a fixed small graph pair by pair, the tiles are the topology previews of the
   server, and the race replays the campaign's mean times on a log-scale clock. With reduced motion,
   each drawing shows its final state. */
(() => {
  'use strict';
  const { $, $$, esc, cssVar, reduceMotion, onTheme, marker, fmtSeconds } = window.TB;
  const DATA = window.LANDING || {};
  const NS = 'http://www.w3.org/2000/svg';
  const svgEl = (tag, attrs = {}, parent = null) => {
    const e = document.createElementNS(NS, tag);
    Object.entries(attrs).forEach(([k, v]) => e.setAttribute(k, v));
    if (parent) parent.appendChild(e);
    return e;
  };
  const rng = (seed) => () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const whenVisible = (el, fn, threshold = 0.3) => {
    if (!('IntersectionObserver' in window)) { fn(); return; }
    const io = new IntersectionObserver((ents) => { if (ents.some((e) => e.isIntersecting)) { io.disconnect(); fn(); } }, { threshold });
    io.observe(el);
  };

  // ── top bar: solid after the hero starts to scroll, current section underlined ──
  const nav = $('#lp-nav');
  const onScroll = () => nav.classList.toggle('is-solid', window.scrollY > 24);
  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();
  if ('IntersectionObserver' in window) {
    const links = new Map($$('.lp-links a').map((a) => [a.getAttribute('href').slice(1), a]));
    const io = new IntersectionObserver((ents) => ents.forEach((e) => {
      if (e.isIntersecting) links.forEach((a, id) => a.classList.toggle('is-current', id === e.target.id));
    }), { rootMargin: '-45% 0px -50% 0px' });
    links.forEach((_, id) => { const s = document.getElementById(id); if (s) io.observe(s); });
  }

  // ── headline: words rise in one after the other ──
  $$('[data-split]').forEach((h) => {
    const words = h.textContent.trim().split(/\s+/);
    h.innerHTML = words.map((w, i) => `<span class="w${/checked/.test(w) ? ' accent' : ''}" style="--w:${i}">${esc(w)}</span>`).join(' ');
    requestAnimationFrame(() => h.classList.add('is-in'));
  });

  // ══ 1 · hero field ══════════════════════════════════════════════════════
  function heroField(canvas) {
    const ctx = canvas.getContext('2d');
    const hud = { iter: $('#hud-iter'), add: $('#hud-new'), total: $('#hud-total'), rule: $('.lp-rule') };
    const STEP = 0.75; // seconds per iteration
    let W = 0; let H = 0; let dpr = 1; let nodes = []; let edges = []; let out = [];
    let colors = {}; let wave = null; let running = true; let last = 0; let pointer = null; let seed = 7;

    const readColors = () => {
      colors = { line: cssVar('--line-strong'), node: cssVar('--ink-3'), accent: cssVar('--accent'), ink: cssVar('--ink') };
    };

    function build() {
      dpr = Math.min(2, window.devicePixelRatio || 1);
      W = canvas.clientWidth; H = canvas.clientHeight;
      canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
      const rand = rng(seed);
      const count = Math.max(36, Math.min(150, Math.round((W * H) / 12500)));
      const cols = Math.max(4, Math.round(Math.sqrt((count * W) / H)));
      const rows = Math.max(3, Math.ceil(count / cols));
      nodes = [];
      for (let r = 0; r < rows; r += 1) {
        for (let c = 0; c < cols; c += 1) {
          if (rand() < 0.16) continue;
          const x = ((c + 0.5 + (rand() - 0.5) * 0.8) * W) / cols;
          const y = ((r + 0.5 + (rand() - 0.5) * 0.8) * H) / rows;
          nodes.push({ id: nodes.length, x, y, bx: x, by: y, dx: 0, dy: 0, ph: rand() * Math.PI * 2, lit: 0 });
        }
      }
      // directed edges to near neighbours, mostly rightwards, plus a few back edges (cycles)
      const has = new Set(); edges = []; out = nodes.map(() => []);
      const add = (a, b) => { const k = `${a}>${b}`; if (a === b || has.has(k)) return; has.add(k); edges.push({ a, b }); out[a].push(b); };
      nodes.forEach((v) => {
        const near = nodes.filter((u) => u !== v && u.bx > v.bx - 12)
          .map((u) => ({ u, d: Math.hypot(u.bx - v.bx, u.by - v.by) }))
          .sort((p, q) => p.d - q.d).slice(0, 4);
        const k = 1 + Math.floor(rand() * 2.4);
        near.slice(0, k).forEach(({ u }) => add(v.id, u.id));
      });
      for (let i = 0; i < Math.round(nodes.length / 14); i += 1) {
        const v = nodes[Math.floor(rand() * nodes.length)];
        const back = nodes.filter((u) => u.bx < v.bx && Math.hypot(u.bx - v.bx, u.by - v.by) < W / 5);
        if (back.length) add(v.id, back[Math.floor(rand() * back.length)].id);
      }
    }

    // breadth-first levels from a start node = the iteration in which tc(s, v) is first derived
    function levels(src) {
      const lv = new Array(nodes.length).fill(-1);
      lv[src] = 0;
      let frontier = [src]; let k = 0;
      while (frontier.length) {
        k += 1;
        const next = [];
        frontier.forEach((u) => out[u].forEach((v) => { if (lv[v] < 0) { lv[v] = k; next.push(v); } }));
        frontier = next;
      }
      return lv;
    }

    function startWave(src) {
      if (src === undefined) {
        // a start node right of the text that reaches many others (on narrow screens, anywhere)
        const lo = W > 900 ? W * 0.42 : 0; const hi = W > 900 ? W * 0.72 : W;
        const cand = nodes.filter((v) => v.bx > lo && v.bx < hi).map((v) => v.id);
        let best = cand[0]; let bestN = -1;
        for (let i = 0; i < 8; i += 1) {
          const c = cand[Math.floor(Math.random() * cand.length)];
          const n = levels(c).filter((x) => x > 0).length;
          if (n > bestN) { best = c; bestN = n; }
        }
        src = best;
      }
      const lv = levels(src);
      const max = Math.max(...lv);
      const byLevel = Array.from({ length: max + 1 }, (_, k) => lv.filter((x) => x === k).length);
      wave = { src, lv, max, byLevel, t: 0, shown: -1, fade: 1 };
    }

    function updateHud(k) {
      if (!wave || k === wave.shown) return;
      wave.shown = k;
      const kk = Math.min(k, wave.max);
      hud.iter.textContent = String(kk);
      hud.add.textContent = String(k === 0 || k > wave.max ? 0 : wave.byLevel[kk] || 0);
      hud.total.textContent = String(wave.byLevel.slice(1, kk + 1).reduce((a, b) => a + b, 0));
      hud.rule.classList.add('pulse');
      setTimeout(() => hud.rule.classList.remove('pulse'), 320);
    }

    function draw(time, dt) {
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, W, H);
      // gentle drift, and nodes give way to the pointer
      nodes.forEach((v) => {
        let tx = Math.sin(time * 0.0004 + v.ph) * 4; let ty = Math.cos(time * 0.00035 + v.ph) * 4;
        if (pointer) {
          const d = Math.hypot(v.bx - pointer.x, v.by - pointer.y);
          if (d < 150) { const f = (1 - d / 150) * 22; tx += ((v.bx - pointer.x) / (d || 1)) * f; ty += ((v.by - pointer.y) / (d || 1)) * f; }
        }
        v.dx += (tx - v.dx) * 0.08; v.dy += (ty - v.dy) * 0.08;
        v.x = v.bx + v.dx; v.y = v.by + v.dy;
      });
      const t = wave ? wave.t : 0;
      const k = wave ? Math.floor(t / STEP) : 0;
      const frac = wave ? (t % STEP) / STEP : 0;

      // the closure so far: arcs from the start node to every node it reaches
      if (wave) {
        const s = nodes[wave.src];
        ctx.lineWidth = 1;
        nodes.forEach((v) => {
          const l = wave.lv[v.id];
          if (l <= 0 || l > k) return;
          const age = Math.min(1, (t - l * STEP) / 1.2);
          ctx.strokeStyle = colors.accent; ctx.globalAlpha = 0.07 + 0.1 * (1 - age) * wave.fade;
          const mx = (s.x + v.x) / 2; const my = (s.y + v.y) / 2 - Math.hypot(v.x - s.x, v.y - s.y) * 0.18;
          ctx.beginPath(); ctx.moveTo(s.x, s.y); ctx.quadraticCurveTo(mx, my, v.x, v.y); ctx.stroke();
        });
      }
      // edges; the ones of the current iteration carry a pulse
      edges.forEach((e) => {
        const a = nodes[e.a]; const b = nodes[e.b];
        const active = wave && wave.lv[e.a] === k && wave.lv[e.b] === k + 1 && k < wave.max;
        const done = wave && wave.lv[e.a] >= 0 && wave.lv[e.a] < k && wave.lv[e.b] >= 0 && wave.lv[e.b] <= k && wave.lv[e.b] === wave.lv[e.a] + 1;
        ctx.globalAlpha = done ? 0.55 * wave.fade + 0.2 : 0.22;
        ctx.strokeStyle = done ? colors.accent : colors.line;
        ctx.lineWidth = done ? 1.4 : 1;
        ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
        if (active) {
          const p = frac; const x = a.x + (b.x - a.x) * p; const y = a.y + (b.y - a.y) * p;
          ctx.globalAlpha = 0.9 * wave.fade; ctx.strokeStyle = colors.accent; ctx.lineWidth = 2;
          ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(x, y); ctx.stroke();
          ctx.fillStyle = colors.accent; ctx.beginPath(); ctx.arc(x, y, 2.6, 0, Math.PI * 2); ctx.fill();
        }
      });
      // nodes: reached ones light up with a ring when they are reached
      nodes.forEach((v) => {
        const l = wave ? wave.lv[v.id] : -1;
        const reached = l >= 0 && l <= k;
        if (reached) {
          const since = t - l * STEP;
          if (since < 0.9) {
            ctx.globalAlpha = (1 - since / 0.9) * 0.5 * wave.fade; ctx.strokeStyle = colors.accent; ctx.lineWidth = 1.5;
            ctx.beginPath(); ctx.arc(v.x, v.y, 4 + since * 26, 0, Math.PI * 2); ctx.stroke();
          }
          ctx.globalAlpha = 0.35 + 0.65 * wave.fade; ctx.fillStyle = colors.accent;
          ctx.beginPath(); ctx.arc(v.x, v.y, l === 0 ? 5 : 3.2, 0, Math.PI * 2); ctx.fill();
        } else {
          ctx.globalAlpha = 0.55; ctx.fillStyle = colors.node;
          ctx.beginPath(); ctx.arc(v.x, v.y, 2.2, 0, Math.PI * 2); ctx.fill();
        }
      });
      ctx.globalAlpha = 1;

      if (wave) {
        updateHud(k);
        wave.t += dt;
        const end = (wave.max + 2.2) * STEP;
        wave.fade = wave.t > end ? Math.max(0, 1 - (wave.t - end) / 1.0) : 1;
        if (wave.t > end + 1.0) startWave();
      }
    }

    function frame(now) {
      if (!running) return;
      const dt = last ? Math.min(0.05, (now - last) / 1000) : 0;
      last = now;
      draw(now, dt);
      requestAnimationFrame(frame);
    }

    function staticFrame() {
      // reduced motion: one finished wave, no movement
      startWave();
      wave.t = (wave.max + 0.5) * STEP; wave.fade = 1;
      draw(0, 0);
      wave.shown = -1; updateHud(wave.max);
    }

    readColors();
    build();
    startWave();
    if (reduceMotion()) { staticFrame(); } else { requestAnimationFrame(frame); }

    let resizeTimer;
    window.addEventListener('resize', () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => { build(); startWave(); if (reduceMotion()) staticFrame(); }, 150);
    });
    onTheme(() => { readColors(); if (reduceMotion()) staticFrame(); });
    const hero = canvas.parentElement;
    hero.addEventListener('pointermove', (e) => { const r = canvas.getBoundingClientRect(); pointer = { x: e.clientX - r.left, y: e.clientY - r.top }; });
    hero.addEventListener('pointerleave', () => { pointer = null; });
    canvas.addEventListener('click', (e) => {
      const r = canvas.getBoundingClientRect(); const x = e.clientX - r.left; const y = e.clientY - r.top;
      const near = nodes.reduce((best, v) => (Math.hypot(v.x - x, v.y - y) < Math.hypot(best.x - x, best.y - y) ? v : best), nodes[0]);
      startWave(near.id);
    });
    // stop drawing while the hero is off screen or the tab is hidden
    const resume = () => { if (!running && !reduceMotion()) { running = true; last = 0; requestAnimationFrame(frame); } };
    if ('IntersectionObserver' in window) {
      new IntersectionObserver((ents) => { if (ents[0].isIntersecting) resume(); else running = false; }).observe(hero);
    }
    document.addEventListener('visibilitychange', () => { if (document.hidden) running = false; else resume(); });
  }
  const field = $('#lp-field');
  if (field) heroField(field);

  // ══ 2 · how a closure grows ═════════════════════════════════════════════
  function growSection() {
    const svg = $('#grow-graph'); const matrix = $('#grow-matrix'); const list = $('#grow-steps');
    if (!svg || !matrix || !list) return;
    // a path 1..5 with a cycle 3 -> 4 -> 5 -> 3, and a branch 2 -> 6 -> 7
    const P = { 1: [40, 130], 2: [118, 130], 3: [198, 58], 4: [300, 58], 5: [300, 168], 6: [198, 205], 7: [380, 214] };
    const E = [[1, 2], [2, 3], [3, 4], [4, 5], [5, 3], [2, 6], [6, 7]];
    const ids = Object.keys(P).map(Number);
    const key = (a, b) => `${a},${b}`;
    const edgeSet = new Set(E.map(([a, b]) => key(a, b)));

    // semi-naive evaluation of tc(X, Z) :- tc(X, Y), e(Y, Z)
    const found = new Map(E.map(([a, b]) => [key(a, b), 0]));
    const iters = [E.map(([a, b]) => [a, b])];
    let delta = iters[0];
    while (delta.length) {
      const next = [];
      delta.forEach(([x, y]) => E.forEach(([y2, z]) => {
        if (y2 === y && !found.has(key(x, z))) { found.set(key(x, z), iters.length); next.push([x, z]); }
      }));
      iters.push(next);
      delta = next;
    }
    const total = found.size;

    // the graph
    const defs = svgEl('defs', {}, svg);
    const mk = svgEl('marker', { id: 'lp-arrow', viewBox: '0 0 8 8', refX: '15', refY: '4', markerWidth: '7', markerHeight: '7', orient: 'auto-start-reverse' }, defs);
    svgEl('path', { d: 'M0 0.6L8 4L0 7.4z' }, mk);
    const arcs = new Map();
    const curve = (a, b, bend) => {
      const [x1, y1] = P[a]; const [x2, y2] = P[b];
      if (a === b) return `M${x1 - 6} ${y1 - 14} a 11 11 0 1 1 12 0`;
      const mx = (x1 + x2) / 2; const my = (y1 + y2) / 2; const dx = x2 - x1; const dy = y2 - y1; const len = Math.hypot(dx, dy) || 1;
      return `M${x1} ${y1} Q ${mx - (dy / len) * bend} ${my + (dx / len) * bend} ${x2} ${y2}`;
    };
    found.forEach((it, k) => {
      if (it === 0) return;
      const [a, b] = k.split(',').map(Number);
      arcs.set(k, svgEl('path', { class: 'gd', d: curve(a, b, 34 + (it % 2) * 18) }, svg));
    });
    E.forEach(([a, b]) => svgEl('path', { class: 'ge', d: a === 5 && b === 3 ? curve(a, b, -22) : `M${P[a][0]} ${P[a][1]} L${P[b][0]} ${P[b][1]}` }, svg));
    const nodeEls = {};
    ids.forEach((v) => {
      const g = svgEl('g', { class: 'gn', transform: `translate(${P[v][0]} ${P[v][1]})` }, svg);
      svgEl('circle', { r: 13 }, g);
      svgEl('text', {}, g).textContent = String(v);
      nodeEls[v] = g;
    });

    // the matrix: row x, column y; a cell is filled when tc(x, y) is derived
    const n = ids.length;
    matrix.style.gridTemplateColumns = `repeat(${n + 1}, minmax(0, 1fr))`;
    const cells = new Map();
    let html = '<span class="lab"></span>' + ids.map((y) => `<span class="lab">${y}</span>`).join('');
    ids.forEach((x) => {
      html += `<span class="lab">${x}</span>`;
      ids.forEach((y) => { html += `<span class="c" data-k="${key(x, y)}" title="tc(${x}, ${y})"></span>`; });
    });
    matrix.innerHTML = html;
    $$('.c', matrix).forEach((c) => cells.set(c.dataset.k, c));
    const count = $('#grow-count');

    // the steps, written from the evaluation itself
    const pairs = (ps) => ps.map(([a, b]) => `(${a}, ${b})`).join(' ');
    const steps = [];
    steps.push({
      title: 'The edges', n: 'base case',
      text: `The base case copies the edges, tc(X, Y) :- e(X, Y). These ${E.length} pairs are the first part of the closure, and they are also the first delta.`,
      pairs: iters[0],
    });
    for (let i = 1; i < iters.length; i += 1) {
      const add = iters[i];
      if (!add.length) {
        steps.push({
          title: 'Fixed point', n: `iteration ${i}`,
          text: `The join of the last delta with the edges gives no pair that is not already known, so the evaluation stops. With ${total} pairs, the closure has ${total - E.length} more than the edges.`,
          pairs: [],
        });
      } else {
        const loops = add.filter(([a, b]) => a === b).map(([a]) => a);
        steps.push({
          title: `Iteration ${i}`, n: `+${add.length}`,
          text: `The ${iters[i - 1].length} pairs found last time are joined with the edges. That gives ${add.length} new pair${add.length > 1 ? 's' : ''}, each connected by a shortest path of ${i + 1} edges.${loops.length ? ` Node${loops.length > 1 ? 's' : ''} ${loops.length > 1 ? `${loops.slice(0, -1).join(', ')} and ${loops[loops.length - 1]}` : loops[0]} now reach${loops.length > 1 ? '' : 'es'} ${loops.length > 1 ? 'themselves' : 'itself'}, because ${loops.length > 1 ? 'they lie' : 'it lies'} on the cycle.` : ''}`,
          pairs: add,
        });
      }
    }
    list.innerHTML = steps.map((s, i) => `<li class="lp-step" data-i="${i}"><h3>${esc(s.title)} <span class="n">${esc(s.n)}</span></h3><p>${esc(s.text)}</p>${s.pairs.length ? `<div class="pairs">${esc(pairs(s.pairs))}</div>` : ''}</li>`).join('');

    let current = -1;
    function show(step) {
      if (step === current) return;
      current = step;
      $$('.lp-step', list).forEach((li) => li.classList.toggle('is-active', +li.dataset.i === step));
      const hot = new Set();
      found.forEach((it, k) => {
        const c = cells.get(k);
        const on = it <= step;
        c.classList.toggle('e', edgeSet.has(k) && on);
        c.classList.toggle('on', on && !edgeSet.has(k));
        c.classList.toggle('new', it === step && step > 0);
        const arc = arcs.get(k);
        if (arc) { arc.classList.toggle('on', on); arc.classList.toggle('new', it === step); }
        if (it === step) k.split(',').forEach((v) => hot.add(+v));
      });
      ids.forEach((v) => nodeEls[v].classList.toggle('hot', hot.has(v)));
      const sofar = [...found.values()].filter((it) => it <= step).length;
      count.textContent = `${sofar} of ${total} pairs`;
    }
    show(0);
    if ('IntersectionObserver' in window) {
      // on narrow screens the drawing sticks to the top, so a step becomes current lower down
      const margin = window.innerWidth < 960 ? '-62% 0px -30% 0px' : '-45% 0px -45% 0px';
      const io = new IntersectionObserver((ents) => ents.forEach((e) => { if (e.isIntersecting) show(+e.target.dataset.i); }), { rootMargin: margin });
      $$('.lp-step', list).forEach((li) => io.observe(li));
    } else {
      show(steps.length - 1);
    }
  }
  growSection();

  // ══ 3 · topology tiles ══════════════════════════════════════════════════
  $$('.lp-shape').forEach((tile) => {
    const p = (DATA.previews || {})[tile.dataset.graph];
    const svg = $('svg', tile);
    if (!p || !svg) return;
    const pos = {}; p.nodes.forEach((v) => { pos[v.id] = [6 + v.x * 88, 6 + v.y * 88]; });
    const r = p.nodes.length > 40 ? 1.4 : p.nodes.length > 16 ? 2 : 2.8;
    const edgeKeys = new Set(p.edges.map((e) => `${e.a},${e.b}`));
    p.closure.forEach((c) => {
      if (edgeKeys.has(`${c.a},${c.b}`) || c.a === c.b || !pos[c.a] || !pos[c.b]) return;
      const [x1, y1] = pos[c.a]; const [x2, y2] = pos[c.b];
      const mx = (x1 + x2) / 2 - (y2 - y1) * 0.2; const my = (y1 + y2) / 2 + (x2 - x1) * 0.2;
      svgEl('path', { class: 'sc', d: `M${x1} ${y1} Q${mx} ${my} ${x2} ${y2}` }, svg);
    });
    p.edges.forEach((e, i) => {
      if (!pos[e.a] || !pos[e.b]) return;
      const [x1, y1] = pos[e.a]; const [x2, y2] = pos[e.b];
      const d = e.self || e.a === e.b ? `M${x1} ${y1 - r} a 3 3 0 1 1 0.1 0` : `M${x1} ${y1} L${x2} ${y2}`;
      const path = svgEl('path', { class: 'se', d, pathLength: '1' }, svg);
      path.style.setProperty('--d', String(Math.min(i * 14, 700)));
    });
    p.nodes.forEach((v, i) => {
      const c = svgEl('circle', { class: 'sn', cx: pos[v.id][0], cy: pos[v.id][1], r }, svg);
      c.style.setProperty('--d', String(Math.min(i * 22, 600)));
    });
    whenVisible(tile, () => tile.classList.add('is-drawn'), 0.35);
  });

  // ══ 4 · the race ════════════════════════════════════════════════════════
  function raceSection() {
    const host = $('#lp-race');
    const facts = DATA.facts;
    if (!host || !facts || !facts.race) return;
    const styles = DATA.styles || {};
    const LIMIT = facts.limit_s || 600;
    const TMIN = 0.01;
    const DURATION = 6.5; // seconds of animation for 0.01 s to the limit
    const lo = Math.log10(TMIN); const hi = Math.log10(LIMIT);
    const pos = (v) => Math.max(0, Math.min(1, (Math.log10(Math.max(v, TMIN)) - lo) / (hi - lo)));
    const why = { timeout: 'time limit', oom: 'out of memory', unsupported: 'not supported', iteration_limit: 'iteration limit', killed: 'killed', error: 'error' };
    let raf = 0;

    function run(graph) {
      cancelAnimationFrame(raf);
      const r = facts.race[graph];
      if (!r) return;
      $('#race-n').textContent = r.n.toLocaleString();
      host.innerHTML = r.entries.map((e) => {
        const st = styles[e.series] || {};
        const color = st.color || cssVar('--ink');
        const label = st.label || e.series;
        const skipped = e.status === 'skipped';
        const title = e.value !== null ? `${label}: ${fmtSeconds(e.value)}` : `${label}: ${skipped ? `not run, after ${why[e.failure] || e.failure} at a smaller n` : why[e.failure] || e.failure}`;
        const reason = e.value === null ? `${why[e.failure] || e.failure}${e.failed_at ? ` at n = ${e.failed_at.toLocaleString()}` : ''}` : '';
        return `<div class="lp-lane" data-series="${esc(e.series)}" style="--lane:${color}" title="${esc(title)}"><span class="who">${marker(st)}<span>${esc(label)}</span></span><div class="track"><div class="bar"></div>${reason ? `<span class="why">${esc(reason)}</span>` : ''}</div><span class="val">0.00 s</span></div>`;
      }).join('') + `<div class="lp-scale"><span></span><div class="ticks">${[0.01, 0.1, 1, 10, 100, LIMIT].map((t) => `<span style="left:${pos(t) * 100}%">${t} s</span>`).join('')}</div><span></span></div>`;
      const lanes = $$('.lp-lane', host).map((el, i) => ({ el, e: r.entries[i], bar: $('.bar', el), val: $('.val', el), done: false }));
      const clock = $('#race-clock');
      let rank = 0;
      const finish = (l) => {
        l.done = true;
        if (l.e.value !== null) {
          rank += 1;
          l.el.classList.add('done');
          l.val.textContent = fmtSeconds(l.e.value);
          l.bar.style.width = `${pos(l.e.value) * 100}%`;
        } else {
          l.el.classList.add('failed');
          l.val.textContent = l.e.status === 'skipped' ? 'not run' : why[l.e.failure] || l.e.failure;
          l.bar.style.width = '100%';
        }
        l.el.classList.remove('running');
      };
      if (reduceMotion()) { lanes.forEach(finish); clock.textContent = fmtSeconds(LIMIT).replace(' s', ''); return; }
      const t0 = performance.now();
      const step = (now) => {
        const p = Math.min(1, (now - t0) / (DURATION * 1000));
        const T = 10 ** (lo + (hi - lo) * p);
        clock.textContent = T < 10 ? T.toFixed(2) : T.toFixed(0);
        lanes.forEach((l) => {
          if (l.done) return;
          const target = l.e.value !== null ? l.e.value : (l.e.status === 'skipped' ? 0 : LIMIT);
          if (l.e.status === 'skipped' && l.e.value === null) { finish(l); return; }
          if (T >= target) { finish(l); return; }
          l.el.classList.add('running');
          l.bar.style.width = `${pos(T) * 100}%`;
          l.val.textContent = T < 10 ? `${T.toFixed(2)} s` : `${T.toFixed(0)} s`;
        });
        if (p < 1) raf = requestAnimationFrame(step);
      };
      raf = requestAnimationFrame(step);
    }

    const seg = $('#race-graph');
    const current = () => ($('[aria-pressed="true"]', seg) || {}).value || Object.keys(facts.race)[0];
    seg.addEventListener('segchange', () => run(current()));
    $('#race-replay').addEventListener('click', () => run(current()));
    whenVisible($('.lp-race-card'), () => run(current()), 0.35);
  }
  raceSection();

  // ══ 5 · the engine diagram ══════════════════════════════════════════════
  function engineDiagram() {
    const svg = $('#lp-engine');
    if (!svg) return;
    const box = (x, y, w, h, label, sub, cls = '') => {
      const g = svgEl('g', { class: `box ${cls}` }, svg);
      svgEl('rect', { x, y, width: w, height: h, rx: 12 }, g);
      svgEl('text', { x: x + w / 2, y: y + h / 2 - (sub ? 8 : 0) }, g).textContent = label;
      if (sub) svgEl('text', { x: x + w / 2, y: y + h / 2 + 12, class: 'sub' }, g).textContent = sub;
      return g;
    };
    const wire = (d) => { svgEl('path', { class: 'wire', d }, svg); svgEl('path', { class: 'flow', d }, svg); };
    const inputs = [['Web UI', 'the wizard'], ['transitive.py', 'size range'], ['benchmark.py', 'size list']];
    inputs.forEach(([l, s], i) => {
      const y = 28 + i * 92;
      wire(`M190 ${y + 28} C 270 ${y + 28} 270 150 350 150`);
      box(20, y, 170, 56, l, s);
    });
    box(350, 108, 220, 84, 'engine/campaign.py', 'CampaignSpec, Campaign.run()', 'core');
    const procs = [];
    for (let i = 0; i < 5; i += 1) {
      const y = 34 + i * 48;
      wire(`M570 150 C 620 150 620 ${y + 16} 660 ${y + 16}`);
      wire(`M770 ${y + 16} C 800 ${y + 16} 800 150 830 150`);
      const g = svgEl('g', { class: 'proc' }, svg);
      svgEl('rect', { x: 660, y, width: 110, height: 32, rx: 8 }, g);
      svgEl('text', { x: 715, y: y + 16 }, g).textContent = `run_one, run ${i + 1}`;
      procs.push(g);
    }
    box(830, 116, 112, 68, 'runs.jsonl', 'one record per run');
    if (reduceMotion()) return;
    let i = 0;
    whenVisible(svg, () => setInterval(() => { procs.forEach((p, j) => p.classList.toggle('on', j === i % procs.length)); i += 1; }, 650), 0.2);
  }
  engineDiagram();
})();
