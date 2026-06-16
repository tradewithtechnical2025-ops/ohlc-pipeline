// ─────────────────────────────────────────────────────────────────────────────
// RRG TAB  —  pgRRG
// Relative Rotation Graph for NSE Sector Indices
// Plug into screener_main.js alongside pgMarket, pgIPO etc.
//
// Dependencies already loaded in your app:
//   - Chart.js (LightweightCharts NOT needed here — we use canvas directly)
//
// Data source: rrg_data.json from R2
// ─────────────────────────────────────────────────────────────────────────────

// ── Constants ────────────────────────────────────────────────────────────────

const RRG_DATA_URL = `${R2_BASE_URL}/rrg_data.json`;   // R2_BASE_URL already defined in your app

const RRG_QUAD = {
    leading:   { color: "#22c55e", label: "Leading"   },
    weakening: { color: "#f59e0b", label: "Weakening" },
    lagging:   { color: "#ef4444", label: "Lagging"   },
    improving: { color: "#3b82f6", label: "Improving" },
};

// Default tail length (weeks)
const RRG_DEFAULT_TAIL = 8;

// ── State ────────────────────────────────────────────────────────────────────

let rrgData        = null;   // full JSON from R2
let rrgBenchmark   = "NIFTY50";
let rrgTailLen     = RRG_DEFAULT_TAIL;
let rrgCanvas      = null;
let rrgCtx         = null;
let rrgTooltipEl   = null;
let rrgAnimFrame   = null;

// ── Page Entry ───────────────────────────────────────────────────────────────

async function pgRRG() {
    const app = document.getElementById("app");
    app.innerHTML = `
      <div style="padding:16px 20px 0;">

        <!-- Header row -->
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:14px;">
          <div>
            <h2 style="margin:0;font-size:17px;font-weight:600;">Relative Rotation Graph</h2>
            <p style="margin:4px 0 0;font-size:12px;color:var(--text-muted,#888);">NSE Sector Indices · JdK RS Ratio vs RS Momentum</p>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
            <select id="rrg-benchmark" style="font-size:12px;padding:5px 10px;border-radius:6px;border:1px solid var(--border,#ddd);background:var(--card-bg,#fff);color:var(--text,#222);">
              <option value="NIFTY50">vs Nifty 50</option>
              <option value="NIF500">vs Nifty 500</option>
              <option value="NIF200">vs Nifty 200</option>
            </select>
            <select id="rrg-tail" style="font-size:12px;padding:5px 10px;border-radius:6px;border:1px solid var(--border,#ddd);background:var(--card-bg,#fff);color:var(--text,#222);">
              <option value="4">4-week tail</option>
              <option value="8" selected>8-week tail</option>
              <option value="12">12-week tail</option>
            </select>
          </div>
        </div>

        <!-- Legend -->
        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;">
          ${Object.entries(RRG_QUAD).map(([k,v]) => `
            <span style="display:flex;align-items:center;gap:5px;font-size:12px;color:var(--text-muted,#888);">
              <span style="width:10px;height:10px;border-radius:50%;background:${v.color};display:inline-block;"></span>
              ${v.label}
            </span>`).join("")}
          <span style="font-size:12px;color:var(--text-muted,#888);margin-left:auto;" id="rrg-generated"></span>
        </div>

        <!-- Canvas wrapper -->
        <div style="position:relative;width:100%;" id="rrg-canvas-wrap">
          <canvas id="rrg-canvas" style="width:100%;display:block;border-radius:8px;"></canvas>
          <div id="rrg-tooltip" style="
            position:absolute;
            background:var(--card-bg,#fff);
            border:1px solid var(--border,#ddd);
            border-radius:8px;
            padding:8px 12px;
            font-size:12px;
            color:var(--text,#222);
            pointer-events:none;
            display:none;
            z-index:10;
            min-width:150px;
            box-shadow:0 2px 8px rgba(0,0,0,0.1);
          "></div>
        </div>

        <!-- Sector table below chart -->
        <div style="margin-top:18px;overflow-x:auto;">
          <table style="width:100%;border-collapse:collapse;font-size:13px;" id="rrg-table">
            <thead>
              <tr style="border-bottom:1px solid var(--border,#ddd);text-align:left;">
                <th style="padding:8px 10px;font-weight:500;color:var(--text-muted,#888);">Sector</th>
                <th style="padding:8px 10px;font-weight:500;color:var(--text-muted,#888);">RS Ratio</th>
                <th style="padding:8px 10px;font-weight:500;color:var(--text-muted,#888);">RS Momentum</th>
                <th style="padding:8px 10px;font-weight:500;color:var(--text-muted,#888);">Quadrant</th>
                <th style="padding:8px 10px;font-weight:500;color:var(--text-muted,#888);">Signal</th>
              </tr>
            </thead>
            <tbody id="rrg-tbody">
              <tr><td colspan="5" style="padding:20px;text-align:center;color:var(--text-muted,#888);">Loading...</td></tr>
            </tbody>
          </table>
        </div>

        <!-- Info note -->
        <p style="font-size:11px;color:var(--text-muted,#888);margin:14px 0 20px;">
          Center (100,100) = Benchmark performance. Bubble size = relative index weight.
          Arrow shows rotation direction. Modified JdK: 10-week / 52-week EMA.
        </p>
      </div>
    `;

    rrgCanvas    = document.getElementById("rrg-canvas");
    rrgCtx       = rrgCanvas.getContext("2d");
    rrgTooltipEl = document.getElementById("rrg-tooltip");

    // Controls
    document.getElementById("rrg-benchmark").addEventListener("change", e => {
        rrgBenchmark = e.target.value;
        rrgRender();
    });
    document.getElementById("rrg-tail").addEventListener("change", e => {
        rrgTailLen = +e.target.value;
        rrgRender();
    });

    // Canvas mouse events
    rrgCanvas.addEventListener("mousemove", rrgOnMouseMove);
    rrgCanvas.addEventListener("mouseleave", () => { rrgTooltipEl.style.display = "none"; });

    // Resize
    window.addEventListener("resize", rrgRender);

    // Load data
    await rrgLoadData();
}

// ── Data Loading ─────────────────────────────────────────────────────────────

async function rrgLoadData() {
    try {
        const res  = await fetch(RRG_DATA_URL);
        rrgData    = await res.json();
        const gen  = rrgData.generated_at || "";
        document.getElementById("rrg-generated").textContent = gen ? `Updated: ${gen}` : "";
        rrgRender();
    } catch (err) {
        console.error("RRG data load failed:", err);
        document.getElementById("rrg-tbody").innerHTML =
            `<tr><td colspan="5" style="padding:20px;text-align:center;color:#ef4444;">Failed to load RRG data. Check R2 pipeline.</td></tr>`;
    }
}

// ── Quadrant Helper ───────────────────────────────────────────────────────────

function rrgGetQuadrant(rs, rm) {
    if (rs >= 100 && rm >= 100) return "leading";
    if (rs >= 100 && rm <  100) return "weakening";
    if (rs <  100 && rm <  100) return "lagging";
    return "improving";
}

function rrgSignal(quad) {
    const map = {
        leading:   "✦ Outperforming & Strengthening",
        weakening: "▼ Outperforming but Slowing",
        lagging:   "✕ Underperforming & Weak",
        improving: "▲ Underperforming but Recovering",
    };
    return map[quad] || "";
}

// ── Coordinate Helpers ────────────────────────────────────────────────────────

function rrgMapX(rs, W, pad, xMin, xMax) {
    return pad + ((rs - xMin) / (xMax - xMin)) * (W - 2 * pad);
}
function rrgMapY(rm, H, pad, yMin, yMax) {
    return H - pad - ((rm - yMin) / (yMax - yMin)) * (H - 2 * pad);
}

// ── Draw ──────────────────────────────────────────────────────────────────────

function rrgRender() {
    if (!rrgData || !rrgCanvas) return;

    const benchData = rrgData.benchmarks?.[rrgBenchmark];
    const sectors   = benchData?.sectors || [];

    // Canvas sizing
    const DPR  = window.devicePixelRatio || 1;
    const cssW = rrgCanvas.parentElement.clientWidth || 700;
    const cssH = Math.max(420, Math.round(cssW * 0.65));
    rrgCanvas.width  = cssW * DPR;
    rrgCanvas.height = cssH * DPR;
    rrgCanvas.style.width  = cssW + "px";
    rrgCanvas.style.height = cssH + "px";
    rrgCtx.setTransform(DPR, 0, 0, DPR, 0, 0);

    const ctx  = rrgCtx;
    const W    = cssW, H = cssH;
    const pad  = Math.round(W * 0.075);

    // Dark mode detection
    const isDark  = window.matchMedia("(prefers-color-scheme: dark)").matches;
    const bgCol   = isDark ? "#111" : "#fff";
    const textCol = isDark ? "#e5e5e5" : "#222";
    const mutedCol= isDark ? "#666" : "#aaa";
    const gridCol = isDark ? "rgba(255,255,255,0.05)" : "rgba(0,0,0,0.05)";

    // Axis range — auto-fit with padding around data, min ±3 from 100
    let allRS = sectors.flatMap(s => s.tail.map(t => t.rs));
    let allRM = sectors.flatMap(s => s.tail.map(t => t.rm));
    if (!allRS.length) { allRS = [97, 103]; allRM = [97, 103]; }
    const margin = 1.5;
    const xMin = Math.min(Math.floor(Math.min(...allRS)) - margin, 97);
    const xMax = Math.max(Math.ceil (Math.max(...allRS)) + margin, 103);
    const yMin = Math.min(Math.floor(Math.min(...allRM)) - margin, 97);
    const yMax = Math.max(Math.ceil (Math.max(...allRM)) + margin, 103);

    const cx = rrgMapX(100, W, pad, xMin, xMax);
    const cy = rrgMapY(100, H, pad, yMin, yMax);

    // Background
    ctx.fillStyle = bgCol;
    ctx.fillRect(0, 0, W, H);

    // Quadrant fills
    const qFills = [
        { x: cx, y: pad,  w: W-pad-cx, h: cy-pad,   col: RRG_QUAD.leading.color,   label: "Leading",   tx: W-pad-8, ty: pad+18, ta: "right" },
        { x: cx, y: cy,   w: W-pad-cx, h: H-pad-cy,  col: RRG_QUAD.weakening.color, label: "Weakening", tx: W-pad-8, ty: H-pad-8, ta: "right" },
        { x: pad,y: cy,   w: cx-pad,   h: H-pad-cy,  col: RRG_QUAD.lagging.color,   label: "Lagging",   tx: pad+8,   ty: H-pad-8, ta: "left"  },
        { x: pad,y: pad,  w: cx-pad,   h: cy-pad,    col: RRG_QUAD.improving.color, label: "Improving", tx: pad+8,   ty: pad+18,  ta: "left"  },
    ];
    qFills.forEach(q => {
        ctx.fillStyle = q.col + "18";
        ctx.fillRect(q.x, q.y, q.w, q.h);
        ctx.fillStyle = q.col + "bb";
        ctx.font = `500 12px -apple-system,system-ui,sans-serif`;
        ctx.textAlign = q.ta;
        ctx.fillText(q.label, q.tx, q.ty);
    });

    // Grid lines
    const gridStep = 2;
    for (let v = Math.ceil(xMin); v <= Math.floor(xMax); v += gridStep) {
        if (v === 100) continue;
        ctx.strokeStyle = gridCol; ctx.lineWidth = 0.5;
        const gx = rrgMapX(v, W, pad, xMin, xMax);
        ctx.beginPath(); ctx.moveTo(gx, pad); ctx.lineTo(gx, H-pad); ctx.stroke();
    }
    for (let v = Math.ceil(yMin); v <= Math.floor(yMax); v += gridStep) {
        if (v === 100) continue;
        ctx.strokeStyle = gridCol; ctx.lineWidth = 0.5;
        const gy = rrgMapY(v, H, pad, yMin, yMax);
        ctx.beginPath(); ctx.moveTo(pad, gy); ctx.lineTo(W-pad, gy); ctx.stroke();
    }

    // Center axes
    ctx.strokeStyle = isDark ? "rgba(255,255,255,0.2)" : "rgba(0,0,0,0.18)";
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(cx, pad); ctx.lineTo(cx, H-pad); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(pad, cy); ctx.lineTo(W-pad, cy); ctx.stroke();

    // Axis labels
    ctx.fillStyle = mutedCol;
    ctx.font = `11px -apple-system,system-ui,sans-serif`;
    ctx.textAlign = "center";
    ctx.fillText("RS Ratio →", W/2, H - Math.round(pad * 0.35));
    ctx.save();
    ctx.translate(Math.round(pad * 0.35), H/2);
    ctx.rotate(-Math.PI/2);
    ctx.fillText("RS Momentum →", 0, 0);
    ctx.restore();

    // Tick labels on axes
    ctx.fillStyle = mutedCol;
    ctx.font = `10px -apple-system,system-ui,sans-serif`;
    [xMin+1, 98, 102, xMax-1].forEach(v => {
        if (v === 100) return;
        const gx = rrgMapX(v, W, pad, xMin, xMax);
        ctx.textAlign = "center";
        ctx.fillText(v.toFixed(0), gx, H - pad + 14);
    });
    [yMin+1, 98, 102, yMax-1].forEach(v => {
        if (v === 100) return;
        const gy = rrgMapY(v, H, pad, yMin, yMax);
        ctx.textAlign = "right";
        ctx.fillText(v.toFixed(0), pad - 6, gy + 4);
    });
    // Center label
    ctx.fillStyle = mutedCol; ctx.textAlign = "center";
    ctx.fillText("100", cx, H - pad + 14);
    ctx.textAlign = "right";
    ctx.fillText("100", pad - 6, cy - 5);

    // ── Draw each sector ─────────────────────────────────────────────────────
    sectors.forEach(s => {
        const tail   = s.tail.slice(-rrgTailLen);
        const pts    = tail.map(t => ({
            x: rrgMapX(t.rs, W, pad, xMin, xMax),
            y: rrgMapY(t.rm, H, pad, yMin, yMax),
        }));
        const latest = tail[tail.length - 1];
        const quad   = rrgGetQuadrant(latest.rs, latest.rm);
        const col    = RRG_QUAD[quad].color;

        // Tail line — fading opacity
        for (let i = 1; i < pts.length; i++) {
            const alpha = 0.12 + 0.65 * (i / (pts.length - 1));
            ctx.strokeStyle = col + Math.round(alpha * 255).toString(16).padStart(2, "0");
            ctx.lineWidth   = 1.8;
            ctx.beginPath();
            ctx.moveTo(pts[i-1].x, pts[i-1].y);
            ctx.lineTo(pts[i].x,   pts[i].y);
            ctx.stroke();
        }

        // Arrowhead at latest point
        if (pts.length >= 2) {
            const p1 = pts[pts.length - 2];
            const p2 = pts[pts.length - 1];
            const angle = Math.atan2(p2.y - p1.y, p2.x - p1.x);
            ctx.save();
            ctx.translate(p2.x, p2.y);
            ctx.rotate(angle);
            ctx.fillStyle = col;
            ctx.beginPath();
            ctx.moveTo(9, 0); ctx.lineTo(-4, -3.5); ctx.lineTo(-4, 3.5);
            ctx.closePath(); ctx.fill();
            ctx.restore();
        }

        // Bubble
        const lp = pts[pts.length - 1];
        const r  = 10;   // fixed radius; can be weight-based if needed
        ctx.beginPath(); ctx.arc(lp.x, lp.y, r, 0, Math.PI * 2);
        ctx.fillStyle   = col + "30"; ctx.fill();
        ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.stroke();

        // Label inside bubble
        ctx.fillStyle  = textCol;
        ctx.font       = `600 10px -apple-system,system-ui,sans-serif`;
        ctx.textAlign  = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(s.name.substring(0, 4), lp.x, lp.y);
        ctx.textBaseline = "alphabetic";
    });

    // Border
    ctx.strokeStyle = isDark ? "rgba(255,255,255,0.08)" : "rgba(0,0,0,0.07)";
    ctx.lineWidth = 0.5;
    ctx.strokeRect(pad, pad, W - 2*pad, H - 2*pad);

    // ── Update table ─────────────────────────────────────────────────────────
    rrgUpdateTable(sectors);
}

// ── Table ─────────────────────────────────────────────────────────────────────

function rrgUpdateTable(sectors) {
    if (!sectors.length) {
        document.getElementById("rrg-tbody").innerHTML =
            `<tr><td colspan="5" style="padding:20px;text-align:center;color:var(--text-muted,#888);">No data for selected benchmark.</td></tr>`;
        return;
    }

    // Sort by quadrant priority: Leading → Improving → Weakening → Lagging
    const order = { leading: 0, improving: 1, weakening: 2, lagging: 3 };
    const sorted = [...sectors].sort((a, b) => {
        const qa = rrgGetQuadrant(a.rs, a.rm);
        const qb = rrgGetQuadrant(b.rs, b.rm);
        return (order[qa] ?? 9) - (order[qb] ?? 9);
    });

    const rows = sorted.map(s => {
        const quad  = rrgGetQuadrant(s.rs, s.rm);
        const col   = RRG_QUAD[quad].color;
        const label = RRG_QUAD[quad].label;
        return `
          <tr style="border-bottom:1px solid var(--border-light,rgba(0,0,0,0.06));">
            <td style="padding:8px 10px;font-weight:500;">${s.name}</td>
            <td style="padding:8px 10px;font-family:monospace;">${s.rs.toFixed(2)}</td>
            <td style="padding:8px 10px;font-family:monospace;">${s.rm.toFixed(2)}</td>
            <td style="padding:8px 10px;">
              <span style="display:inline-flex;align-items:center;gap:5px;">
                <span style="width:8px;height:8px;border-radius:50%;background:${col};"></span>
                ${label}
              </span>
            </td>
            <td style="padding:8px 10px;font-size:12px;color:var(--text-muted,#888);">${rrgSignal(quad)}</td>
          </tr>`;
    }).join("");

    document.getElementById("rrg-tbody").innerHTML = rows;
}

// ── Hover Tooltip ─────────────────────────────────────────────────────────────

function rrgOnMouseMove(e) {
    if (!rrgData || !rrgCanvas) return;

    const benchData = rrgData.benchmarks?.[rrgBenchmark];
    const sectors   = benchData?.sectors || [];
    if (!sectors.length) return;

    const rect = rrgCanvas.getBoundingClientRect();
    const mx   = e.clientX - rect.left;
    const my   = e.clientY - rect.top;

    const W    = rrgCanvas.clientWidth;
    const H    = rrgCanvas.clientHeight;
    const pad  = Math.round(W * 0.075);

    let allRS = sectors.flatMap(s => s.tail.map(t => t.rs));
    let allRM = sectors.flatMap(s => s.tail.map(t => t.rm));
    if (!allRS.length) return;
    const margin = 1.5;
    const xMin = Math.min(Math.floor(Math.min(...allRS)) - margin, 97);
    const xMax = Math.max(Math.ceil (Math.max(...allRS)) + margin, 103);
    const yMin = Math.min(Math.floor(Math.min(...allRM)) - margin, 97);
    const yMax = Math.max(Math.ceil (Math.max(...allRM)) + margin, 103);

    let found = null;
    sectors.forEach(s => {
        const lp  = s.tail[s.tail.length - 1];
        const lx  = rrgMapX(lp.rs, W, pad, xMin, xMax);
        const ly  = rrgMapY(lp.rm, H, pad, yMin, yMax);
        const hit = Math.hypot(mx - lx, my - ly);
        if (hit < 18) found = { s, lx, ly, lp };
    });

    if (found) {
        const { s, lx, ly, lp } = found;
        const quad  = rrgGetQuadrant(lp.rs, lp.rm);
        const col   = RRG_QUAD[quad].color;
        const label = RRG_QUAD[quad].label;

        // Tail change: compare first and last in current tail window
        const tail      = s.tail.slice(-rrgTailLen);
        const first     = tail[0];
        const rsChange  = (lp.rs - first.rs).toFixed(2);
        const rmChange  = (lp.rm - first.rm).toFixed(2);
        const rsArrow   = rsChange >= 0 ? "▲" : "▼";
        const rmArrow   = rmChange >= 0 ? "▲" : "▼";

        rrgTooltipEl.style.display = "block";
        rrgTooltipEl.style.left    = (lx + 16) + "px";
        rrgTooltipEl.style.top     = (ly - 20) + "px";
        rrgTooltipEl.innerHTML     = `
          <div style="font-weight:600;margin-bottom:5px;color:${col};">${s.name}</div>
          <div style="display:grid;grid-template-columns:auto auto;gap:2px 12px;font-size:11px;">
            <span style="color:var(--text-muted,#888);">RS Ratio</span>
            <span style="font-family:monospace;">${lp.rs.toFixed(2)} <small style="color:${rsChange>=0?'#22c55e':'#ef4444'}">${rsArrow}${Math.abs(rsChange)}</small></span>
            <span style="color:var(--text-muted,#888);">RS Mom</span>
            <span style="font-family:monospace;">${lp.rm.toFixed(2)} <small style="color:${rmChange>=0?'#22c55e':'#ef4444'}">${rmArrow}${Math.abs(rmChange)}</small></span>
            <span style="color:var(--text-muted,#888);">Quadrant</span>
            <span style="color:${col};">${label}</span>
            <span style="color:var(--text-muted,#888);">As of</span>
            <span>${lp.date}</span>
          </div>`;
    } else {
        rrgTooltipEl.style.display = "none";
    }
}

// ── Cleanup on page leave ─────────────────────────────────────────────────────
// Call this when navigating away from pgRRG (if your router has a teardown hook)
function pgRRGDestroy() {
    window.removeEventListener("resize", rrgRender);
    if (rrgCanvas) {
        rrgCanvas.removeEventListener("mousemove", rrgOnMouseMove);
    }
    if (rrgAnimFrame) cancelAnimationFrame(rrgAnimFrame);
}
