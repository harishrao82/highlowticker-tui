#!/usr/bin/env python3
"""
BTC probability surface web viewer.

Serves an interactive chart at http://localhost:7332

Views:
  - Heatmap: full P(Up) surface — x=time, y=delta, color=probability
  - Slice by time: fix time elapsed, see P(Up) vs delta as a curve
  - Slice by delta: fix delta, see P(Up) evolve over 15 minutes

Usage:
    python3 btc_surface_viewer.py
    python3 btc_surface_viewer.py --port 8080
"""
import argparse
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# Set by CLI args at startup
_surface_file: Path = Path.home() / ".btc_model_surface.json"

app = FastAPI()


def _load_surface() -> dict:
    if not _surface_file.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Surface file not found: {_surface_file}"
        )
    return json.loads(_surface_file.read_text())


@app.get("/api/surface")
def api_surface():
    return JSONResponse(_load_surface())


@app.get("/", response_class=HTMLResponse)
def index():
    if not _surface_file.exists():
        return HTMLResponse("""
        <h2 style="font-family:monospace;padding:40px">
        Surface file not found.<br><br>
        Run first: <code>python3 btc_model_builder.py</code>
        </h2>""")

    surface = _load_surface()
    meta    = surface["meta"]
    built   = meta.get("built_at", "?")[:19].replace("T", " ")
    ticker  = meta.get("ticker", "BTC")
    n_obs   = meta.get("n_obs", 0)
    n_count = meta.get("n_windows", meta.get("n_days", 0))
    t_max   = meta.get("day_sec", 900)
    t_ref   = 5400 if ticker == "SPY" else 90   # SPY: 1hr in, BTC: 90s in

    # For SPY, format time label as HH:MM; for BTC as Ns
    if ticker == "SPY":
        ref_label = "t=1hr"
    else:
        ref_label = f"t={t_ref}s"

    return HTMLResponse(f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{ticker} Probability Surface</title>
<script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0d1117; color: #e6edf3; font-family: 'Courier New', monospace; }}
  #header {{ padding: 16px 24px; background: #161b22; border-bottom: 1px solid #30363d;
             display: flex; align-items: center; gap: 24px; flex-wrap: wrap; }}
  #header h1 {{ font-size: 16px; color: #58a6ff; letter-spacing: 1px; }}
  .meta {{ font-size: 11px; color: #8b949e; }}
  #controls {{ padding: 14px 24px; background: #161b22; border-bottom: 1px solid #30363d;
               display: flex; align-items: center; gap: 32px; flex-wrap: wrap; }}
  .ctrl-group {{ display: flex; align-items: center; gap: 10px; }}
  label {{ font-size: 12px; color: #8b949e; white-space: nowrap; }}
  .tab-btn {{ padding: 6px 16px; border-radius: 6px; border: 1px solid #30363d;
              background: transparent; color: #8b949e; cursor: pointer; font-size: 12px;
              font-family: monospace; transition: all .15s; }}
  .tab-btn.active {{ background: #1f6feb; border-color: #1f6feb; color: #fff; }}
  input[type=range] {{ width: 200px; accent-color: #58a6ff; cursor: pointer; }}
  .val-badge {{ font-size: 13px; color: #58a6ff; min-width: 70px; }}
  #chart {{ width: 100%; height: calc(100vh - 130px); }}
  #no-conf-overlay {{ display: none; position: absolute; top: 0; right: 0;
                      background: rgba(13,17,23,.7); padding: 6px 10px;
                      font-size: 11px; color: #8b949e; border-radius: 0 0 0 6px; }}
</style>
</head>
<body>

<div id="header">
  <h1>{ticker} PROBABILITY SURFACE</h1>
  <span class="meta">
    {n_count:,} {'days' if ticker == 'SPY' else 'windows'} &nbsp;·&nbsp;
    {n_obs:,} training obs &nbsp;·&nbsp;
    built {built}
  </span>
</div>

<div id="controls">
  <div class="ctrl-group">
    <button class="tab-btn active" onclick="setView('heatmap')">Heatmap</button>
    <button class="tab-btn" onclick="setView('time')">Fix time →</button>
    <button class="tab-btn" onclick="setView('delta')">Fix delta →</button>
  </div>

  <div class="ctrl-group" id="slider-time-group" style="display:none">
    <label>Time elapsed:</label>
    <input type="range" id="slider-time" min="0" max="{t_max}" step="{meta.get('t_step', 15)}" value="{t_ref}"
           oninput="onSliderTime(this.value)">
    <span class="val-badge" id="val-time">{t_ref}s</span>
  </div>

  <div class="ctrl-group" id="slider-delta-group" style="display:none">
    <label>Delta from open:</label>
    <input type="range" id="slider-delta" min="{int(meta.get('d_min', -1.5) * 100)}" max="{int(meta.get('d_max', 1.5) * 100)}" step="1" value="10"
           oninput="onSliderDelta(this.value)">
    <span class="val-badge" id="val-delta">+0.10%</span>
  </div>

  <div class="ctrl-group">
    <label>
      <input type="checkbox" id="hide-low-conf" onchange="render()" checked>
      &nbsp;Hide low confidence (n&lt;10)
    </label>
  </div>
</div>

<div style="position:relative">
  <div id="chart"></div>
</div>

<script>
const T_MAX   = {t_max};
const T_REF   = {t_ref};
const REF_LABEL = '{ref_label}';
let surface, tVals, dVals, meta;
let currentView = 'heatmap';

async function loadData() {{
  const resp = await fetch('/api/surface');
  const data = await resp.json();
  surface = data.surface;   // surface[ti][di] = {{p_up, n_eff}}
  tVals   = data.t_vals;    // length n_t
  dVals   = data.d_vals;    // length n_d
  meta    = data.meta;
  render();
}}

function hideLowConf() {{ return document.getElementById('hide-low-conf').checked; }}

function setView(v) {{
  currentView = v;
  document.querySelectorAll('.tab-btn').forEach((b,i) => {{
    b.classList.toggle('active', ['heatmap','time','delta'][i] === v);
  }});
  document.getElementById('slider-time-group').style.display  = (v === 'time')  ? 'flex' : 'none';
  document.getElementById('slider-delta-group').style.display = (v === 'delta') ? 'flex' : 'none';
  render();
}}

function onSliderTime(v) {{
  document.getElementById('val-time').textContent = v + 's';
  render();
}}

function onSliderDelta(v) {{
  const pct = (v / 100).toFixed(2);
  const sign = v >= 0 ? '+' : '';
  document.getElementById('val-delta').textContent = sign + pct + '%';
  render();
}}

/* ── Heatmap ──────────────────────────────────────────────────────── */
function renderHeatmap() {{
  const mask = hideLowConf();
  const z = [], text = [], t_labels = [];

  // z[di][ti] for plotly heatmap (y=delta, x=time)
  for (let di = 0; di < dVals.length; di++) {{
    const row_z = [], row_t = [];
    for (let ti = 0; ti < tVals.length; ti++) {{
      const cell = surface[ti][di];
      const neff = cell.n_eff;
      const val  = (mask && neff < 10) ? null : cell.p_up;
      row_z.push(val);
      row_t.push(`t=${{tVals[ti]}}s  δ=${{dVals[di] >= 0 ? '+' : ''}}${{dVals[di].toFixed(2)}}%<br>P(Up)=${{(cell.p_up*100).toFixed(1)}}%  n_eff=${{neff}}`);
    }}
    z.push(row_z);
    text.push(row_t);
  }}

  const trace = {{
    type:        'heatmap',
    x:           tVals,
    y:           dVals,
    z:           z,
    text:        text,
    hovertemplate: '%{{text}}<extra></extra>',
    colorscale: [
      [0.00, '#1a3a6b'],
      [0.30, '#2d6a9f'],
      [0.45, '#4a90c4'],
      [0.50, '#e8e8e8'],
      [0.55, '#f5a623'],
      [0.70, '#e06c00'],
      [1.00, '#c0392b'],
    ],
    zmid:  0.5,
    zmin:  0.0,
    zmax:  1.0,
    colorbar: {{
      title: 'P(Up)',
      tickformat: '.0%',
      len: 0.8,
    }},
  }};

  const layout = {{
    paper_bgcolor: '#0d1117',
    plot_bgcolor:  '#0d1117',
    font:  {{ color: '#e6edf3', family: 'Courier New', size: 11 }},
    xaxis: {{ title: 'Time elapsed in window (seconds)', gridcolor: '#21262d', zeroline: false }},
    yaxis: {{ title: 'Delta from open (%)', gridcolor: '#21262d', zeroline: true,
              zerolinecolor: '#444c56', zerolinewidth: 1 }},
    margin: {{ l: 60, r: 20, t: 20, b: 60 }},
    shapes: [{{
      type: 'line', x0: T_REF, x1: T_REF, y0: dVals[0], y1: dVals[dVals.length-1],
      line: {{ color: '#58a6ff', width: 1, dash: 'dot' }},
    }}],
    annotations: [{{
      x: T_REF, y: dVals[dVals.length-1], xanchor: 'left', yanchor: 'top',
      text: REF_LABEL, showarrow: false, font: {{ color: '#58a6ff', size: 10 }},
    }}],
  }};

  Plotly.react('chart', [trace], layout, {{responsive: true, displayModeBar: false}});
}}

/* ── Fix time: P(Up) vs delta curve ──────────────────────────────── */
function renderTimeSlice() {{
  const t_target = parseInt(document.getElementById('slider-time').value);
  const ti = tVals.findIndex(v => v >= t_target);
  const mask = hideLowConf();
  const t_actual = tVals[Math.max(0, ti)];

  const x = [], y_up = [], y_dn = [], customdata = [];
  for (let di = 0; di < dVals.length; di++) {{
    const cell = surface[Math.max(0,ti)][di];
    if (mask && cell.n_eff < 10) continue;
    x.push(dVals[di]);
    y_up.push(cell.p_up);
    y_dn.push(1 - cell.p_up);
    customdata.push(cell.n_eff);
  }}

  const traceUp = {{
    x, y: y_up, name: 'P(Up)', type: 'scatter', mode: 'lines',
    line: {{ color: '#e06c00', width: 2 }},
    customdata,
    hovertemplate: 'δ=%{{x:.2f}}%  P(Up)=%{{y:.1%}}  n_eff=%{{customdata}}<extra></extra>',
  }};
  const traceDn = {{
    x, y: y_dn, name: 'P(Down)', type: 'scatter', mode: 'lines',
    line: {{ color: '#2d6a9f', width: 2 }},
    customdata,
    hovertemplate: 'δ=%{{x:.2f}}%  P(Down)=%{{y:.1%}}  n_eff=%{{customdata}}<extra></extra>',
  }};
  const trace50 = {{
    x: [dVals[0], dVals[dVals.length-1]], y: [0.5, 0.5],
    type: 'scatter', mode: 'lines', name: 'fair value',
    line: {{ color: '#444c56', width: 1, dash: 'dash' }}, showlegend: false,
  }};

  const layout = {{
    paper_bgcolor: '#0d1117', plot_bgcolor: '#0d1117',
    font: {{ color: '#e6edf3', family: 'Courier New', size: 11 }},
    title: {{ text: `P(Up) vs delta  —  fixed at t=${{t_actual}}s into window`,
              font: {{ size: 13, color: '#58a6ff' }} }},
    xaxis: {{ title: 'Delta from window open (%)', gridcolor: '#21262d', zeroline: true,
              zerolinecolor: '#444c56' }},
    yaxis: {{ title: 'Probability', gridcolor: '#21262d', range: [0, 1],
              tickformat: '.0%' }},
    legend: {{ bgcolor: '#161b22', bordercolor: '#30363d', borderwidth: 1 }},
    margin: {{ l: 60, r: 20, t: 50, b: 60 }},
  }};

  Plotly.react('chart', [trace50, traceUp, traceDn], layout,
    {{responsive: true, displayModeBar: false}});
}}

/* ── Fix delta: P(Up) vs time curve ──────────────────────────────── */
function renderDeltaSlice() {{
  const d_raw    = parseInt(document.getElementById('slider-delta').value) / 100;
  const d_target = Math.round(d_raw * 100) / 100;
  const di_raw   = dVals.findIndex(v => Math.round(v * 100) === Math.round(d_target * 100));
  const di       = Math.max(0, di_raw);
  const mask     = hideLowConf();
  const d_actual = dVals[di];

  const x = [], y_up = [], y_dn = [], customdata = [];
  for (let ti = 0; ti < tVals.length; ti++) {{
    const cell = surface[ti][di];
    if (mask && cell.n_eff < 10) continue;
    x.push(tVals[ti]);
    y_up.push(cell.p_up);
    y_dn.push(1 - cell.p_up);
    customdata.push(cell.n_eff);
  }}

  const sign = d_actual >= 0 ? '+' : '';
  const traceUp = {{
    x, y: y_up, name: 'P(Up)', type: 'scatter', mode: 'lines+markers',
    line: {{ color: '#e06c00', width: 2 }}, marker: {{ size: 4 }},
    customdata,
    hovertemplate: 't=%{{x}}s  P(Up)=%{{y:.1%}}  n_eff=%{{customdata}}<extra></extra>',
  }};
  const traceDn = {{
    x, y: y_dn, name: 'P(Down)', type: 'scatter', mode: 'lines+markers',
    line: {{ color: '#2d6a9f', width: 2 }}, marker: {{ size: 4 }},
    customdata,
    hovertemplate: 't=%{{x}}s  P(Down)=%{{y:.1%}}  n_eff=%{{customdata}}<extra></extra>',
  }};
  const trace50 = {{
    x: [0, 900], y: [0.5, 0.5], type: 'scatter', mode: 'lines',
    line: {{ color: '#444c56', width: 1, dash: 'dash' }}, showlegend: false,
  }};
  const traceArrow = {{
    x: [T_REF, T_REF], y: [0, 1], type: 'scatter', mode: 'lines',
    line: {{ color: '#58a6ff', width: 1, dash: 'dot' }},
    name: REF_LABEL, showlegend: true,
  }};

  const layout = {{
    paper_bgcolor: '#0d1117', plot_bgcolor: '#0d1117',
    font: {{ color: '#e6edf3', family: 'Courier New', size: 11 }},
    title: {{ text: `P(Up) over time  —  fixed delta = ${{sign}}${{Math.abs(d_actual).toFixed(2)}}%`,
              font: {{ size: 13, color: '#58a6ff' }} }},
    xaxis: {{ title: 'Seconds elapsed', gridcolor: '#21262d',
              zeroline: false, range: [0, T_MAX] }},
    yaxis: {{ title: 'Probability', gridcolor: '#21262d', range: [0, 1],
              tickformat: '.0%' }},
    legend: {{ bgcolor: '#161b22', bordercolor: '#30363d', borderwidth: 1 }},
    margin: {{ l: 60, r: 20, t: 50, b: 60 }},
  }};

  Plotly.react('chart', [trace50, traceArrow, traceUp, traceDn], layout,
    {{responsive: true, displayModeBar: false}});
}}

function render() {{
  if (!surface) return;
  if (currentView === 'heatmap') renderHeatmap();
  else if (currentView === 'time') renderTimeSlice();
  else renderDeltaSlice();
}}

loadData();
</script>
</body>
</html>""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port",    type=int, default=7332)
    parser.add_argument("--host",    default="127.0.0.1")
    parser.add_argument("--surface", default=None,
                        help="Path to surface JSON (default: ~/.btc_model_surface.json)")
    args = parser.parse_args()

    if args.surface:
        _surface_file = Path(args.surface).expanduser()

    ticker = _load_surface().get("meta", {}).get("ticker", "BTC")
    print(f"{ticker} Surface Viewer → http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
