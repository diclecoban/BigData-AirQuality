# Dashboard UI Merge — Design Spec
**Date:** 2026-06-06  
**Status:** Approved by user

## Goal

Replace `dashboard/index.html` with a new file that looks exactly like `uidesign.html` while preserving all live functionality from the current `dashboard/index.html`.

## Source Files

| File | Role |
|---|---|
| `uidesign.html` (363 lines) | Visual design source — exact layout, colors, Tailwind classes |
| `dashboard/index.html` (631 lines) | Functionality source — Leaflet, Chart.js, SSE, API calls, JS logic |
| `dashboard/server.py` | Backend unchanged — no modifications needed |

## Changes to Make

### 1. HTML Base: uidesign.html
Take `uidesign.html` as the structural base. All Tailwind classes, card layouts, KPI rows, spacing, colors stay exactly as-is.

### 2. CDN Imports (add to `<head>`)
- Leaflet CSS + JS (already in dashboard/index.html)
- Chart.js (already in dashboard/index.html)
- Keep existing Tailwind + Google Fonts

### 3. Map Section
**Remove:** `<div class="absolute inset-0 opacity-40 ...">` (Wikipedia image)  
**Remove:** All static SVG dot overlays  
**Keep:** Outer container `class="flex-grow bg-[#EFF6FF] rounded-card relative overflow-hidden ... border border-nav-border"`  
**Add inside:** `<div id="map"></div>` filling 100% width/height  
**Keep:** Legend overlay div (bottom-left, same style as uidesign.html)  
**Add CSS:** `#map { width: 100%; height: 100%; }`

### 4. Chart Placeholders → Canvas Elements
Replace SVG placeholder blocks with `<canvas>` elements, keeping same card height/structure:
- `<canvas id="chart-daily">` in "AQI Daily Trend" card
- `<canvas id="chart-pollutants">` in "Pollutant Breakdown" card  
- `<canvas id="chart-hourly">` in "24-Hour AQI Cycle" card
- Remove static legend from Pollutant card (Chart.js legend will render at bottom)

### 5. Dynamic `id` Attributes on Existing Elements
Add `id`s to all elements that receive live data:

| Element | id |
|---|---|
| Clock `14:22:05` | `id="clock"` |
| KAFKA status dot | `id="dot-kafka"` |
| KAFKA status label | `id="lbl-kafka"` |
| CSV REPLAY label → SOURCE | `id="lbl-source"` |
| Alert banner div | `id="alert-banner"` + `class="hidden ..."` |
| City AQI value | `id="kpi-city-aqi"` |
| City AQI sub-text | `id="kpi-city-aqi-sub"` |
| Active Stations value | `id="kpi-stations"` |
| Best Model R² value | `id="kpi-r2"` |
| Best Model R² sub-text | `id="kpi-r2-sub"` |
| Msgs/sec value | `id="kpi-msgs"` |
| Msgs/sec sub-text | `id="kpi-msgs-sub"` |
| Highest AQI value | `id="kpi-highest"` |
| Highest AQI sub-text | `id="kpi-highest-sub"` |
| Station count label | `id="station-count-label"` |
| Live feed list | `id="live-feed"` |
| Live feed footer | `id="live-feed-footer"` |
| Stations by AQI container | `id="stations-by-aqi"` |
| Model cards grid | `id="model-cards"` |
| Model bars row | `id="model-bars"` |
| Kafka pipeline badge | `id="pipe-kafka"` |

### 6. Replace Hardcoded Values with Loading States
- KPI number divs: `—` instead of `31.9`, `28`, `0.953`, `1.7`, `144`
- KPI sub-texts: `Loading…` or `Connecting…`
- Alert banner: starts with `class="hidden"`
- Live feed: `<div class="text-center text-text-muted text-xs py-8">Loading station data…</div>`
- Stations by AQI container: `<div class="text-text-muted text-xs">Loading…</div>`
- Model cards: `<div class="text-text-muted text-xs col-span-2">Loading…</div>`

### 7. JavaScript Block
Copy the entire `<script>` block from `dashboard/index.html` verbatim (lines 247–629). It handles:
- AQI color/label helpers
- Clock tick
- Leaflet map initialization + marker management
- Live station state + feed rendering
- KPI updates + alert banner
- `/api/latest` initial load
- `/api/status` polling
- SSE `/stream/live` connection
- Chart.js chart building (daily, pollutants, hourly)
- Stations by AQI bar list
- Model performance cards
- Bootstrap sequence

## What Does NOT Change
- `dashboard/server.py` — no changes
- `dashboard/Dockerfile` — no changes
- All Tailwind color names and class names from uidesign.html
- Pipeline Status section structure (just add `id="pipe-kafka"` to Kafka badge)
- Model Performance section structure (replace hardcoded cards with `id` containers)
- Bottom row layout

## Success Criteria
1. Page looks visually identical to `uidesign.html` when opened in browser
2. Real Leaflet map renders with Istanbul tiles and station markers
3. KPI cards populate with real data from `/api/latest`
4. Live feed sidebar shows station readings in real time
5. Chart.js charts render from `/api/daily`, `/api/hourly`
6. Clock ticks every second
7. Pipeline status updates from `/api/status`
8. Alert banner shows/hides based on live AQI data
