# Duct — Plug-and-Play Data Pipeline Engine

**Duct** is a lightweight, TOML-configured data pipeline engine for Python. It handles task scheduling, dependency resolution, parallel execution, retry logic, timeout enforcement, and result persistence — so you can focus on writing the logic.

---

## Features

- **TOML pipeline definitions** — define pipelines in a single `.toml` file
- **Dependency-aware scheduling** — topological sorting, tasks run as soon as dependencies complete
- **Parallel execution** — configurable worker pool (default 4) for independent tasks
- **Priority-based ordering** — lower priority value = runs first within a batch
- **Retry with exponential backoff** — configurable retry count and delay per task
- **Timeout enforcement** — per-task or global timeout override, worker thread is released on timeout
- **Result persistence** — persist task results to disk (JSON first, pickle fallback), reloaded on resume
- **Cross-run resumption** — re-running with the same run ID skips already-completed tasks
- **SQLite state store** — persistent task status, duration, and attempt count across runs
- **Per-task logging** — file + console logs per task, grouped under `output_base/logs/{run_id}/`
- **Environment variable substitution** — `${VAR_NAME}` syntax in TOML params
- **Graceful shutdown** — SIGINT/SIGTERM handling cancels pending tasks cleanly

---

## Installation

```bash
pip install duct        # or: pip install -e .
```

Requirements: Python 3.11+ (uses `tomllib` from stdlib, `graphlib` from stdlib).

---

## Quick Start

### 1. Define your tasks

```python
# tasks.py

def extract_sales(ctx, **kwargs):
    return [
        {"date": "2025-01-01", "amount": 100},
        {"date": "2025-01-02", "amount": 150},
    ]

def clean_sales(extract_sales, ctx, **kw):
    return [r for r in extract_sales if r["amount"] > 0]

def load_sales(clean_sales, ctx, mode="overwrite", **kw):
    import csv
    out = ctx.output_base / "sales_clean.csv"
    with open(out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "amount"])
        writer.writeheader()
        writer.writerows(clean_sales)
    print(f"Wrote {len(clean_sales)} rows to {out}")
```

### 2. Define your pipeline

```toml
# pipeline.toml

[name]
name = "sales_etl"

[tasks.extract_sales]
type = "extract"
function = "tasks:extract_sales"
priority = 5
timeout = 30

[tasks.clean_sales]
type = "transform"
depends_on = ["extract_sales"]
function = "tasks:clean_sales"
retry = 2
retry_delay = 1.5
result_persist = true

[tasks.load_sales]
type = "load"
depends_on = ["clean_sales"]
function = "tasks:load_sales"
params = { mode = "overwrite" }
```

### 3. Run

```bash
python -m duct pipeline.toml
```

---

## CLI Reference

```bash
duct PIPELINE_FILE [options]

Options:
  --mode {prod,test}       Execution mode (default: prod)
                           prod → ./output,  test → ./test_output
  --max-workers N          Max concurrent tasks (default: 4)
  --run-id ID              Resume a specific run by its run_id
  --timeout SECONDS        Override all task timeouts globally
  --log-level LEVEL        DEBUG, INFO, WARNING, ERROR (default: INFO)
```

---

## Pipeline Configuration Reference

### `[name]`
| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Human-readable pipeline identifier |

### `[tasks.*]`
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | enum | — | `extract`, `transform`, `quality`, `load` |
| `function` | string | — | Fully qualified function (`module:func` or `module.func`) |
| `depends_on` | list[string] | `[]` | Task names that must complete first |
| `params` | table | `{}` | Static parameters passed to the function |
| `retry` | integer | `0` | Number of retries on failure |
| `retry_delay` | float | `1.0` | Base delay in seconds (doubles each retry) |
| `priority` | integer | `10` | Lower value = higher priority (runs first) |
| `timeout` | integer | `3600` | Max seconds before task is marked failed |
| `result_persist` | bool | `false` | Save result to disk for cross-run reuse |

### Environment Variable Substitution

```toml
[tasks.load_db]
params = { host = "${DB_HOST}", port = "${DB_PORT:5432}" }
```

`${VAR}` substitutes from the environment. `${VAR:default}` uses `default` if unset.

---

## Architecture

```
pipeline.toml  ──load_pipeline──►  Pipeline (list of Tasks)
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    ▼                                             ▼
            build_sorter()                              pipeline_session()
            TopologicalSorter                           (creates StateStore,
                   │                                     ResultStore, dirs)
                   ▼                                             │
            run_pipeline()  ◄───────────────────────────────  (state, store)
                   │
           ThreadPoolExecutor(max_workers=N)
                   │
           ┌───────┴───────────────────────┐
           ▼                               ▼
    run_single_task()               run_single_task() ...
           │
           ▼
    threading.Thread(target=execute_task)
           │ join(timeout=task.timeout)
           ▼
    execute_task()
           ├── state.is_completed() → skip / load persisted
           ├── _resolve_function()    → callable
           ├── _validate_params()    → check signature
           ├── func(**call_kwargs)   → run user code
           ├── store[task.name] = result
           └── _persist_result()     → JSON or pickle
```

### Key Components

| Module | Responsibility |
|--------|---------------|
| `config.py` | Load and parse TOML pipeline files |
| `models.py` | `Task`, `Pipeline`, `RunContext`, `TaskType` dataclasses |
| `graph.py` | Build `TopologicalSorter` from task dependencies |
| `state.py` | SQLite-backed `StateStore` — tracks run status |
| `executor.py` | `execute_task()` — resolve, validate, run, persist |
| `runner.py` | `run_pipeline()`, `run_single_task()`, `pipeline_session()` |
| `file_utils.py` | Path helpers (`get_results_dir`, `ensure_partition_path`, `atomic_write`) |
| `logging_utils.py` | `setup_task_logger()` — file + console per task |
| `formatting.py` | `wrap_log()`, `format_summary()` — CLI output |
| `secrets.py` | `generate_run_id()` — cryptographically random run IDs |

---

## Example: Complex ETL Pipeline

See `pipeline.toml` in this repo for a 15-task ETL example:

```
extract_products ─┬─► enrich_products ─┬─► aggregate_revenue ──► quality_aggregate
                  │                     │
extract_customers ┘                     │
                  ├─► enrich_customers ─┤
                  │                     │
extract_orders   ─┴─► transform_orders ──┼─► merge_orders_product ─► aggregate_by_customer
                                                    │
                                                    └─► lineage_check
                                                                       │
                                                                       ▼
                                              ┌─ prepare_final_dataset ◄─┘
                                              │
                                              └─► final_quality_check ──► export_report
```

---

## Testing

```bash
python -m unittest discover tests/ -v
```

**103 tests across 5 modules:**

| Test Module | Coverage |
|-------------|----------|
| `test_config.py` | TOML loading, env var substitution, task construction |
| `test_models.py` | Dataclass immutability, defaults, type enums |
| `test_graph.py` | Topological sorting, dependency validation, cycle detection |
| `test_state.py` | SQLite read/write, concurrency, multi-run isolation |
| `test_executor.py` | Function resolution, param validation, persistence, result store |
| `test_runner.py` | Priority sorting, session lifecycle, empty pipelines |
| `test_file_utils.py` | Path construction, atomic writes, partition cleanup |
| `test_integration.py` | Full pipeline execution, retries, timeouts, resumption |
| `test_stress.py` | 50–100 parallel tasks, diamond DAGs, deep chains, concurrent sessions, resource usage |

---

## Result Persistence

Tasks with `result_persist = true` save their return value to disk after each run:

```
output_base/
└── tmp/
    └── {run_id}/
        └── results/
            ├── task_a.json
            ├── task_b.json
            └── task_c.pkl    ← fallback if JSON serialization fails
```

On re-run (same `run_id`), completed tasks are skipped and the persisted result is loaded into the `ResultStore`.

---

## Graceful Shutdown

Sending `SIGINT` (Ctrl+C) or `SIGTERM` sets a flag. On the next loop iteration, all pending futures are cancelled and the process exits cleanly.

---

## Output Structure

```
output_base/
├── duct.db              ← SQLite state store
├── logs/
│   └── {run_id}/
│       ├── task_a.log
│       ├── task_b.log
│       └── ...
├── tmp/
│   └── {run_id}/
│       └── results/
│           ├── a.json
│           └── b.pkl
└── (your task outputs)
```