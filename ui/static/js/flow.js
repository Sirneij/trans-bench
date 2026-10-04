/* Closure flow: the overview's live drawing of what a transitive closure computes.
   A layered graph (with a few back edges, so it has cycles) is drawn left to right. From a source node the
   reachable set grows one breadth-first wave at a time, the same waves a semi-naive evaluation of
   tc(X, Z) :- tc(X, Y), e(Y, Z) adds per iteration. Pulses travel along the edges, reached nodes light up and
   the pairs the closure adds arc in. Hovering a node starts a wave from it. */
(() => {
  'use strict';
  const NS = 'http://www.w3.org/2000/svg';
  const el = (tag, attrs = {}, parent) => {
    const e = document.createElementNS(NS, tag);
    Object.entries(attrs).forEach(([k, v]) => e.setAttribute(k, v));
    if (parent) parent.appendChild(e);
    return e;
  };
  const rng = (seed) => () => { seed |= 0; seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };

  function build(seed, W, H) {
    const rand = rng(seed);
    const layers = 7; const nodes = []; const byLayer = [];
    for (let l = 0; l < layers; l += 1) {
      const k = l === 0 ? 2 : 2 + Math.floor(rand() * 3);
      byLayer.push([]);
      for (let j = 0; j < k; j += 1) {
        const x = 46 + (l * (W - 92)) / (layers - 1) + (rand() - 0.5) * 18;
        const y = ((j + 1) * H) / (k + 1) + (rand() - 0.5) * 26;
        const v = { id: nodes.length, x, y, layer: l };
        nodes.push(v); byLayer[l].push(v);
      }
    }
    const edges = []; const has = new Set();
    const add = (a, b, back = false) => { const key = `${a.id}>${b.id}`; if (a === b || has.has(key)) return; has.add(key); edges.push({ a, b, back }); };
    for (let l = 0; l < layers - 1; l += 1) {
      byLayer[l].forEach((a) => {
        const next = byLayer[l + 1];
        add(a, next[Math.floor(rand() * next.length)]);
        if (rand() < 0.55) add(a, next[Math.floor(rand() * next.length)]);
        if (l < layers - 2 && rand() < 0.22) { const n2 = byLayer[l + 2]; add(a, n2[Math.floor(rand() * n2.length)]); }
      });
      byLayer[l + 1].forEach((b) => { if (!edges.some((e) => e.b === b)) add(byLayer[l][Math.floor(rand() * byLayer[l].length)], b); });
    }
    // two back edges: cycles, so some nodes reach themselves
    for (let i = 0; i < 2; i += 1) {
      const from = byLayer[4 + i][Math.floor(rand() * byLayer[4 + i].length)];
      const to = byLayer[1 + i][Math.floor(rand() * byLayer[1 + i].length)];
      add(from, to, true);
    }
    return { nodes, edges };
  }

  function edgePath(e) {
    const { a, b } = e;
    if (e.back) { // sweep under the graph
      const dip = Math.max(a.y, b.y) + 70;
      return `M${a.x} ${a.y} C${a.x + 30} ${dip} ${b.x - 30} ${dip} ${b.x} ${b.y}`;
    }
    const dx = (b.x - a.x) * 0.5;
    return `M${a.x} ${a.y} C${a.x + dx} ${a.y} ${b.x - dx} ${b.y} ${b.x} ${b.y}`;
  }

  class ClosureFlow {
    constructor(host, { seed = 7, readout } = {}) {
      this.host = host; this.readout = readout;
      this.W = 720; this.H = 380;
      this.g = build(seed, this.W, this.H);
      this.svg = el('svg', { viewBox: `0 0 ${this.W} ${this.H}`, class: 'flow', role: 'img', 'aria-label': 'Animated graph: reachability spreading from a source node, one iteration at a time' });
      this.lTC = el('g', { class: 'f-tc' }, this.svg);
      this.lE = el('g', { class: 'f-edges' }, this.svg);
      this.lP = el('g', { class: 'f-pulses' }, this.svg);
      this.lN = el('g', { class: 'f-nodes' }, this.svg);
      this.g.edges.forEach((e, i) => { e.el = el('path', { d: edgePath(e), class: `f-e${e.back ? ' back' : ''}`, style: `--i:${i}` }, this.lE); e.len = e.el.getTotalLength(); });
      this.g.nodes.forEach((v, i) => {
        v.g = el('g', { class: 'f-n', transform: `translate(${v.x} ${v.y})`, style: `--i:${i}`, tabindex: '-1' }, this.lN);
        el('circle', { r: 15, class: 'f-hit' }, v.g);
        v.ring = el('circle', { r: 7, class: 'f-ring' }, v.g);
        el('circle', { r: 7, class: 'f-dot' }, v.g);
        v.label = el('text', { y: -14, class: 'f-label' }, v.g);
        v.label.textContent = `v${v.id}`;
        v.g.addEventListener('pointerenter', () => this.start(v, true));
      });
      host.appendChild(this.svg);
      this.pulses = []; this.timers = []; this.visible = true; this.manual = false;
      this.reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      this.raf = this.raf.bind(this);
      if ('IntersectionObserver' in window) new IntersectionObserver(([en]) => { this.visible = en.isIntersecting; if (this.visible && !this.running) this.next(); }).observe(host);
      document.addEventListener('visibilitychange', () => { if (!document.hidden && !this.running) this.next(); });
      host.addEventListener('pointerleave', () => { this.manual = false; });
      requestAnimationFrame(this.raf);
      setTimeout(() => this.next(), 700);
    }

    bfs(src) {
      const out = new Map(this.g.nodes.map((v) => [v, []]));
      this.g.edges.forEach((e) => out.get(e.a).push(e));
      const dist = new Map([[src, 0]]); const waves = []; let frontier = [src]; let d = 0;
      const self = { reached: false };
      while (frontier.length) {
        d += 1; const wave = []; const next = [];
        frontier.forEach((u) => out.get(u).forEach((e) => {
          if (e.b === src && !self.reached) { self.reached = true; wave.push({ e, to: src, d, self: true }); }
          if (!dist.has(e.b)) { dist.set(e.b, d); next.push(e.b); wave.push({ e, to: e.b, d }); }
        }));
        if (wave.length) waves.push(wave);
        frontier = next;
      }
      return { waves, reached: dist.size - 1 + (self.reached ? 1 : 0), cyclic: self.reached };
    }

    clear() {
      this.timers.forEach(clearTimeout); this.timers = [];
      this.pulses.forEach((p) => p.dot.remove()); this.pulses = [];
      this.lTC.replaceChildren();
      this.g.edges.forEach((e) => e.el.classList.remove('lit'));
      this.g.nodes.forEach((v) => { v.g.classList.remove('src', 'hit', 'dim'); });
    }

    later(fn, ms) { this.timers.push(setTimeout(fn, ms)); }

    start(src, manual = false) {
      if (manual) this.manual = true;
      this.clear();
      this.running = true;
      const { waves, reached, cyclic } = this.bfs(src);
      src.g.classList.add('src');
      this.g.nodes.forEach((v) => { if (v !== src) v.g.classList.add('dim'); });
      const step = this.reduce ? 0 : manual ? 420 : 760;
      let count = 0;
      this.say(src, 0, 0, waves.length, cyclic);
      waves.forEach((wave, k) => {
        this.later(() => {
          wave.forEach(({ e, to, self }) => {
            e.el.classList.add('lit');
            this.pulse(e, step * 0.85, () => {
              to.g.classList.remove('dim'); to.g.classList.add('hit');
              if (!self) this.arc(src, to); else to.g.classList.add('cyc');
              count += 1;
              this.say(src, k + 1, count, waves.length, cyclic);
            });
          });
        }, k * step);
      });
      const total = waves.length * step + (this.reduce ? 0 : 2200);
      this.later(() => { this.running = false; if (!this.manual) this.next(); }, total);
    }

    next() {
      if (!this.visible || document.hidden || this.manual || this.reduce && this.done) return;
      const candidates = this.g.nodes.filter((v) => v.layer <= 2);
      this.cursor = ((this.cursor ?? -1) + 1) % candidates.length;
      this.done = true;
      this.start(candidates[this.cursor]);
    }

    pulse(e, ms, done) {
      if (this.reduce || ms === 0) { done(); return; }
      const dot = el('circle', { r: 3.2, class: 'f-pulse' }, this.lP);
      this.pulses.push({ dot, e, t0: performance.now(), ms, done });
    }

    arc(a, b) {
      const mx = (a.x + b.x) / 2; const lift = Math.min(150, 30 + Math.abs(b.x - a.x) * 0.32);
      const p = el('path', { d: `M${a.x} ${a.y - 7} Q${mx} ${Math.min(a.y, b.y) - lift} ${b.x} ${b.y - 7}`, class: 'f-arc', pathLength: '1' }, this.lTC);
      return p;
    }

    raf(now) {
      this.pulses = this.pulses.filter((p) => {
        const t = Math.min(1, (now - p.t0) / p.ms);
        const e = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
        const pt = p.e.el.getPointAtLength(p.e.len * e);
        p.dot.setAttribute('cx', pt.x); p.dot.setAttribute('cy', pt.y);
        if (t >= 1) { p.dot.remove(); p.done(); return false; }
        return true;
      });
      requestAnimationFrame(this.raf);
    }

    say(src, iter, count, iters, cyclic) {
      if (!this.readout) return;
      this.readout.innerHTML = `<span><b>v${src.id}</b> source</span><span>iteration <b>${iter}</b> / ${iters}</span><span>reaches <b>${count}</b> node${count === 1 ? '' : 's'}${cyclic && iter === iters ? ', itself included' : ''}</span>`;
    }
  }

  window.ClosureFlow = ClosureFlow;
})();
