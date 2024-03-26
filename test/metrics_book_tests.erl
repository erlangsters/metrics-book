%%
%% Copyright (c) 2026, Byteplug LLC.
%%
%% This source file is part of a project made by the Erlangsters community and
%% is released under the MIT license. Please refer to the LICENSE.md file that
%% can be found at the root of the project repository.
%%
%% Written by Jonathan De Wachter <jonathan.dewachter@byteplug.io>
%%
-module(metrics_book_tests).
-include_lib("eunit/include/eunit.hrl").

declare_inc_set_errors_test() ->
    {ok, Pid} = metrics_book:open(memory),
    {error, {unknown_metric, hits}} = metrics_book:inc(Pid, hits),
    ok = metrics_book:declare(Pid, hits, counter),
    ok = metrics_book:inc(Pid, hits),
    {error, {wrong_kind, hits, counter}} = metrics_book:set(Pid, hits, 1),
    {error, {negative_increment, -1}} = metrics_book:inc(Pid, hits, -1),
    ok = metrics_book:declare(Pid, temp, gauge),
    ok = metrics_book:set(Pid, temp, 21.5),
    {error, {wrong_kind, temp, gauge}} = metrics_book:inc(Pid, temp),
    ok = metrics_book:close(Pid).

declare_idempotent_and_conflict_test() ->
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, hits, counter, [{labels, [method]}]),
    ok = metrics_book:declare(Pid, hits, counter, [{labels, [method]}]),
    {error, {already_declared, hits}} =
        metrics_book:declare(Pid, hits, gauge, [{labels, [method]}]),
    {error, {labels_mismatch, hits, [method]}} =
        metrics_book:declare(Pid, hits, counter, [{labels, [path]}]),
    ok = metrics_book:close(Pid).

value_after_inc_test() ->
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, hits, counter),
    {ok, undefined} = metrics_book:value(Pid, hits),
    ok = metrics_book:inc(Pid, hits, 3),
    {ok, 3} = metrics_book:value(Pid, hits),
    ok = metrics_book:inc(Pid, hits, 2),
    {ok, 5} = metrics_book:value(Pid, hits),
    ok = metrics_book:close(Pid).

query_samples_over_time_test() ->
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, temp, gauge, [{labels, [room]}]),
    ok = metrics_book:set(Pid, 1, temp, #{room => a}, 10),
    ok = metrics_book:set(Pid, 2, temp, #{room => a}, 20),
    {ok, Samples} = metrics_book:query(Pid, #{name => temp}),
    2 = length(Samples),
    [#{value := 10}, #{value := 20}] = Samples,
    ok = metrics_book:close(Pid).

label_canonicalize_test() ->
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, hits, counter, [{labels, [a]}]),
    ok = metrics_book:inc(Pid, hits, #{a => <<"x">>}, 1),
    ok = metrics_book:inc(Pid, hits, #{a => x}, 1),
    {ok, 2} = metrics_book:value(Pid, hits, #{a => x}),
    ok = metrics_book:close(Pid).

reopen_declare_restore_test() ->
    File = tmp_file(),
    {ok, Pid} = metrics_book:open({file, File}),
    ok = metrics_book:declare(Pid, hits, counter),
    ok = metrics_book:inc(Pid, 10, hits, #{}, 5),
    ok = metrics_book:close(Pid),
    {ok, Pid2} = metrics_book:open({file, File}),
    {error, {unknown_metric, hits}} = metrics_book:inc(Pid2, hits),
    ok = metrics_book:declare(Pid2, hits, counter),
    {ok, 5} = metrics_book:value(Pid2, hits),
    ok = metrics_book:declare(Pid2, never, counter),
    {ok, undefined} = metrics_book:value(Pid2, never),
    ok = metrics_book:close(Pid2).

explicit_time_and_write_error_test() ->
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, hits, counter),
    ok = metrics_book:inc(Pid, 10, hits, #{}, 1),
    {error, {out_of_order, 5, 10}} = metrics_book:inc(Pid, 5, hits, #{}, 1),
    {ok, 1} = metrics_book:value(Pid, hits),
    ok = metrics_book:close(Pid).

stream_samples_test() ->
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, hits, counter),
    ok = metrics_book:inc(Pid, hits),
    {ok, Ref} = metrics_book:stream(Pid, #{name => hits}, self()),
    {metrics_book, Ref, {sample, #{value := 1}}} = recv(),
    {metrics_book, Ref, live} = recv(),
    ok = metrics_book:inc(Pid, hits),
    {metrics_book, Ref, {sample, #{value := 2}}} = recv(),
    ok = metrics_book:cancel(Pid, Ref),
    {metrics_book, Ref, closed} = recv(),
    Sink = spawn_link(fun() -> receive go -> sink_loop([]) end end),
    ok = metrics_book:inc(Pid, hits),
    ok = metrics_book:inc(Pid, hits),
    ok = metrics_book:inc(Pid, hits),
    {ok, Pid2} = metrics_book:open(memory, [{max_mailbox, 1}]),
    ok = metrics_book:declare(Pid2, hits, counter),
    lists:foreach(fun(_) -> ok = metrics_book:inc(Pid2, hits) end, lists:seq(1, 5)),
    {ok, Ref2} = metrics_book:stream(Pid2, #{}, Sink),
    timer:sleep(20),
    Sink ! go,
    Msgs = drain_sink(Sink),
    true = lists:any(
        fun
            ({metrics_book, R, {overflow, N}}) when R =:= Ref2, N > 0 ->
                true;
            (_) ->
                false
        end,
        Msgs
    ),
    ok = metrics_book:close(Pid),
    ok = metrics_book:close(Pid2).

stream_to_in_past_no_live_fanout_test() ->
    flush(),
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, hits, counter),
    ok = metrics_book:inc(Pid, 100, hits, #{}, 1),
    T2 = 200,
    true = T2 < erlang:system_time(millisecond),
    {ok, Ref} = metrics_book:stream(Pid, #{to => T2}, self()),
    {metrics_book, Ref, {sample, #{value := 1, time := 100}}} = recv(),
    {metrics_book, Ref, live} = recv(),
    ok = metrics_book:inc(Pid, hits),
    receive
        {metrics_book, Ref, {sample, _}} ->
            error(got_live_fanout)
    after 100 ->
        ok
    end,
    ok = metrics_book:close(Pid).

folder_restore_newest_test() ->
    Dir = tmp_dir(),
    {ok, Pid} = metrics_book:open(
        {folder, Dir},
        [{page_lines, 100}, {max_pages, 2}]
    ),
    ok = metrics_book:declare(Pid, hits, counter),
    lists:foreach(
        fun(N) ->
            ok = metrics_book:inc(Pid, N, hits, #{}, 1)
        end,
        lists:seq(1, 150)
    ),
    {ok, 150} = metrics_book:value(Pid, hits),
    ok = metrics_book:close(Pid),
    {ok, Pid2} = metrics_book:open({folder, Dir}),
    ok = metrics_book:declare(Pid2, hits, counter),
    {ok, Last} = metrics_book:value(Pid2, hits),
    true = Last >= 100,
    ok = metrics_book:close(Pid2).

required_label_errors_test() ->
    {ok, Pid} = metrics_book:open(memory),
    ok = metrics_book:declare(Pid, hits, counter, [{labels, [method]}]),
    {error, {invalid_label, method, missing}} = metrics_book:inc(Pid, hits, 1),
    {error, {invalid_label, extra, extra}} =
        metrics_book:inc(Pid, hits, #{method => get, extra => 1}, 1),
    ok = metrics_book:inc(Pid, hits, #{method => get}, 1),
    {ok, Samples} = metrics_book:query(Pid, #{name => hits}),
    1 = length(Samples),
    ok = metrics_book:close(Pid).

flush() ->
    receive
        _ ->
            flush()
    after 0 ->
        ok
    end.

recv() ->
    receive
        M ->
            M
    after 1000 ->
        error(timeout)
    end.

sink_loop(Acc) ->
    receive
        {dump, From} ->
            From ! {sink, lists:reverse(Acc)};
        Msg ->
            sink_loop([Msg | Acc])
    end.

drain_sink(Pid) ->
    Pid ! {dump, self()},
    receive
        {sink, Msgs} ->
            Msgs
    after 1000 ->
        []
    end.

tmp_file() ->
    filename:join(tmp_dir(), "metrics.bin").

tmp_dir() ->
    string:chomp(os:cmd("mktemp -d")).
