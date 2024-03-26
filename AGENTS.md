# metrics-book

- Pure Erlang library in the `observability` family.
- OTP 27, 28, and 29. `rebar3` is the build. Library app: `kernel` / `stdlib` / `book_storage`, no `{mod, ...}`.
- One `gen_server` per open book. Public `open/1,2` is `gen_server:start_link`. Callers put `open` in their own supervisor.
- `declare/3,4` is process-lifetime and required on every start. Samples are durable; the declare table is not.
- v1 is `counter` and `gauge` only. No histograms, no `observe/3`, no OTP logger handler.
- Default `{max_pages, 32}` on memory and folder. The option is omitted on `{file, Path}`. Prefer `{folder, Path}` for any long-lived process.
