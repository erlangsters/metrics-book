# Local Metrics

[![Erlangsters Repository](https://img.shields.io/badge/erlangsters-metrics--book-%23a90432)](https://github.com/erlangsters/metrics-book)
![Supported Erlang/OTP Versions](https://img.shields.io/badge/erlang%2Fotp-27%7C28%7C29-%23a90432)
![Current Version](https://img.shields.io/badge/version-0.0.1-%23354052)
![License](https://img.shields.io/github/license/erlangsters/metrics-book)
[![Build Status](https://img.shields.io/github/actions/workflow/status/erlangsters/metrics-book/build.yml)](https://github.com/erlangsters/metrics-book/actions/workflows/build.yml)
[![Documentation Link](https://img.shields.io/badge/documentation-available-yellow)](http://erlangsters.github.io/metrics-book/)

This 0.0.1 is a candidate implementation. The API may change in 0.0.2.

Local metrics is a simple library for keeping application metrics locally.

It is a thin process facade on top of `book-storage`. v1 is counters and gauges. Every successful `inc`/`set` appends a sample. Prefer `{folder, Path}` as the persistent target.

```erlang
{ok, Pid} = metrics_book:open({folder, "metrics"}),
ok = metrics_book:declare(Pid, http_requests, counter, [{labels, [method]}]),
ok = metrics_book:inc(Pid, http_requests, #{method => get}, 1),
{ok, 1} = metrics_book:value(Pid, http_requests, #{method => get}).
```

Written by the Erlangsters [community](https://about.erlangsters.org/) and released under the MIT [license](https://opensource.org/license/mit).

## Getting started

`metrics_book:open/1` starts a linked gen_server that owns one book. Declare every metric on every start. Samples survive reopen; the declare table does not.

```erlang
{ok, Pid} = metrics_book:open({folder, "metrics"}),
ok = metrics_book:declare(Pid, players, gauge),
ok = metrics_book:set(Pid, players, 12),
{ok, Samples} = metrics_book:query(Pid, #{name => players}),
ok = metrics_book:close(Pid).
```

Use `{folder, Path}` when the data should survive restart and stay bounded. Default `max_pages` is 32 on memory and folder. Use `{file, Path}` only as unbounded debug persist. Use `memory` in tests.

`query/2` does not require declare and returns samples over time, oldest-first. `stream/3` replies `{ok, Ref}`, then historical matches, then `{metrics_book, Ref, live}`, then later matching samples. A slow subscriber may see `{overflow, N}`. There is no durable cursor.

Callers who want a supervised book put `{metrics_book, open, [Target, Options]}` in their supervisor.

## Installing the library

To use metrics-book in a rebar3 project, add it to your rebar.config.

```erlang
{deps, [
  {metrics_book, {git, "https://github.com/erlangsters/metrics-book.git", {tag, "0.0.1"}}}
]}.
```
