# Rust Job Runner

![CI](https://github.com/berat-552/rust-job-runner/actions/workflows/ci.yml/badge.svg?branch=main)

A small background job service written in Rust. You send it jobs over HTTP, a few workers run them in the background, and you can ask what happened to each job.

A learning project, built to practise Rust and concurrent programming. It shows how a job queue works: shared state between workers, retries with backoff, backpressure, and shutting down without losing work in progress.

## What it does

1. You `POST` a job to the server. It gets an id and goes into a queue.
2. Three workers take jobs from the queue and run them at the same time.
3. If a job fails, it is retried up to 3 times, waiting a bit longer each time (100 ms, 200 ms, 400 ms).
4. You `GET` a job's id at any point to see if it is pending, running, completed or failed. `GET /stats` shows how many jobs are in each status.
5. If the queue is full (10 jobs waiting), new jobs are refused with a "try again later" response.
6. Press Ctrl-C and the server stops taking requests, running jobs finish, and the program exits cleanly.

Everything is held in memory. Restarting the server clears all jobs.

## Run it

You need Rust installed.

```
cargo run
```

The server listens on `http://127.0.0.1:3000`. Run the tests with:

```
cargo test
```

## Try it (PowerShell)

Open a second terminal while the server is running.

Submit a job:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:3000/jobs -ContentType 'application/json' -Body '{"payload":"ok"}'
```

You get back the job's id. Check on it:

```powershell
Invoke-RestMethod -Uri http://127.0.0.1:3000/jobs/1
```

The status is one of `pending`, `running`, `completed` or `failed`.

The demo worker treats a few payloads specially so you can see retries:

- `ok` (or anything else): succeeds after about half a second.
- `flaky`: fails twice, then succeeds.
- `broken`: always fails, and is given up on after 3 retries.

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:3000/jobs -ContentType 'application/json' -Body '{"payload":"flaky"}'
```

Poll a job to watch it change state (change `2` to the id the previous command returned):

```powershell
1..12 | ForEach-Object { Invoke-RestMethod -Uri http://127.0.0.1:3000/jobs/2; Start-Sleep -Milliseconds 300 }
```

A `broken` job works the same way. Post `{"payload":"broken"}` and poll its id: it retries a few times, then ends as `failed`.

Look up a job that doesn't exist (returns 404):

```powershell
try { Invoke-RestMethod -Uri http://127.0.0.1:3000/jobs/999 } catch { $_.Exception.Response.StatusCode }
```

See how many jobs are in each status:

```powershell
Invoke-RestMethod -Uri http://127.0.0.1:3000/stats
```

Fill the queue to see it push back (the last few are refused with `ServiceUnavailable`):

```powershell
1..15 | ForEach-Object { try { Invoke-RestMethod -Method Post -Uri http://127.0.0.1:3000/jobs -ContentType 'application/json' -Body '{"payload":"ok"}' } catch { "refused: " + $_.Exception.Response.StatusCode } }
```

See a clean shutdown: post a few jobs, then press Ctrl-C in the server window. Running jobs finish, then it prints a summary and how many jobs were left in the queue.

## API

| Request | Result |
| --- | --- |
| `POST /jobs` with `{"payload": "..."}` | `201` with `{"id": N}`. `503` if the queue is full. |
| `GET /jobs/{id}` | `200` with `{"id": N, "status": "..."}`. `404` if the id is unknown. `400` if the id isn't a number. |
| `GET /stats` | `200` with `{"pending": N, "running": N, "completed": N, "failed": N}`. Counts every job since the server started. A job waiting to be retried counts as `running`. |

## What each file does

- **`main.rs`**: starts everything. It creates the queue (max 10 waiting jobs), starts 3 workers with a demo job handler, runs the HTTP server, and handles Ctrl-C. After the server stops, it shuts the queue down, waits for the workers, and prints a summary.
- **`job.rs`**: the `Job` itself: its id, payload, status and retry count. It only allows sensible status changes (for example, a job can't complete without running first), decides whether a job may be retried, and works out how long to wait before a retry.
- **`queue.rs`**: the waiting line of jobs. It hands jobs out first-in, first-out, refuses new jobs when full, remembers the status of every job and can count how many are in each status, and can be closed or shut down. It also has some older single-threaded processing helpers that the tests still use.
- **`worker.rs`**: the worker loop. Each worker takes a job, runs it, and records the result. Failed jobs wait out their backoff and go back in the queue. When the queue is empty but still open, the worker waits and checks again. It exits when the queue is closed and empty.
- **`api.rs`**: the HTTP side. It defines the three endpoints, gives each new job a unique id, and turns queue results into HTTP responses.

## How it stays safe

The queue is shared between the workers and the HTTP handlers behind a lock. The rule is simple: take the lock briefly to get or return a job, and never hold it while a job is running or waiting. That means one slow job never blocks the others, and two workers can never take the same job.

## Known limitations

- Jobs and statuses live in memory only. A restart loses everything, and the status record grows with every job.
- A job waiting to be retried shows as `running`, because its worker is still holding it during the wait.
- A worker sits idle while its failed job waits for the backoff.
- Idle workers check for new jobs every 20 ms instead of being woken up instantly.
- Retries can push the queue past its limit of 10, because a retry is existing work and not a new job.
- On Ctrl-C, jobs still waiting in the queue are not run. The program reports how many were left.
- The job handler, port, worker count and queue size are hard-coded in `main.rs`.
