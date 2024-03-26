# Metrics Book Copilot Guidelines

- `metrics-book` is a `pure-erlang-library` repository in the `observability` family.
- Build with `rebar3`. Do not reintroduce Erlang.mk or Makefile-based usage.
- It is a gen_server facade on top of `book-storage`. One process owns one book. Callers start it with `open/1,2`.
- v1 is counters and gauges. Every successful `inc`/`set` appends a sample. `declare/3,4` is process-lifetime and required on every start.
- Production persistence is `{folder, Path}` with default `{max_pages, 32}`. `{file, Path}` is unbounded debug persist. `memory` is for tests.
- Keep the design local-first and simple. No histograms, no OTP logger handler, no Prometheus, and no high write volume.
- Keep examples and API shaping Erlang-first. Prefer BEAM-neutral public one-liners.
