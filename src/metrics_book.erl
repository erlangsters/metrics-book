%%
%% Copyright (c) 2026, Byteplug LLC.
%%
%% This source file is part of a project made by the Erlangsters community and
%% is released under the MIT license. Please refer to the LICENSE.md file that
%% can be found at the root of the project repository.
%%
%% Written by Jonathan De Wachter <jonathan.dewachter@byteplug.io>
%%
-module(metrics_book).
-moduledoc """
Process facade for local counter and gauge samples on top of `book_storage`.

Every successful `inc`/`set` appends a sample. `declare/3,4` is process-lifetime
and required on every start. Samples are durable.

```erlang
{ok, Pid} = metrics_book:open({folder, "metrics"}),
ok = metrics_book:declare(Pid, http_requests, counter, [{labels, [method]}]),
ok = metrics_book:inc(Pid, http_requests, #{method => get}, 1).
```

Callers who want a supervised book put `{metrics_book, open, [Target, Options]}`
in their supervisor. Prefer `{folder, Path}` for any long-lived process.
""".
-behaviour(gen_server).

-export_type([
    server/0,
    target/0,
    option/0,
    name/0,
    kind/0,
    label_value/0,
    labels/0,
    declare_option/0,
    sample/0,
    query/0,
    stream_ref/0,
    reason/0
]).

-export([
    open/1, open/2,
    close/1,
    declare/3, declare/4,
    inc/2, inc/3, inc/4, inc/5,
    set/3, set/4, set/5,
    value/2, value/3,
    query/2,
    stream/3,
    cancel/2,
    info/1,
    sync/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(CALL_TIMEOUT, 5000).
-define(VERSION, 1).
-define(MAX_RESTORE, 1000000).

-record(stream, {
    ref :: reference(),
    pid :: pid(),
    mon :: reference(),
    name :: undefined | name(),
    labels :: #{atom() => binary()},
    silent = false :: boolean(),
    overflow = 0 :: non_neg_integer()
}).

-record(state, {
    book :: book_storage:book(),
    last_time :: empty | book_storage:time(),
    max_mailbox :: pos_integer(),
    streams = [] :: [#stream{}],
    declared = #{} :: #{name() => {kind(), any | [atom()]}},
    values = #{} :: #{{name(), #{atom() => binary()}} => number()},
    restore_values = #{} :: #{{name(), #{atom() => binary()}} => number()},
    restore_kinds = #{} :: #{name() => kind()}
}).

-doc """
Pid or locally registered name of a metrics book.
""".
-type server() :: pid() | atom().

-doc """
Storage target, the same term `book_storage:open/1` accepts.
""".
-type target() :: book_storage:target().

-doc """
Metric name. Kind is per name, not per series.
""".
-type name() :: atom().

-doc """
Sample kind. v1 is `counter` and `gauge` only.
""".
-type kind() :: counter | gauge.

-doc """
Accepted label value before canonicalization to binary.
""".
-type label_value() :: atom() | integer() | unicode:chardata().

-doc """
User labels. The keys `name` and `kind` are reserved.
""".
-type labels() :: #{atom() => label_value()}.

-doc """
Declare option. `{labels, Keys}` is the required key set for `inc` / `set` / `value`.
""".
-type declare_option() :: {labels, [atom()]}.

-doc """
One sample returned by query and stream.
""".
-type sample() :: #{
    time := book_storage:time(),
    name := name(),
    kind := kind(),
    labels := #{atom() => binary()},
    value := number()
}.

-doc """
Query or stream filter.

Required keys from `declare` are not applied here. Omitted label keys match
any series of that name.
""".
-type query() :: #{
    name => name(),
    labels => labels(),
    from => book_storage:time() | beginning,
    to => book_storage:time() | latest,
    last => non_neg_integer(),
    limit => pos_integer()
}.

-doc """
Open option: book-storage options plus process options.
""".
-type option() ::
    book_storage:option() |
    {name, atom()} |
    {timeout, timeout()} |
    {max_mailbox, pos_integer()}.

-doc """
Opaque stream identifier returned by `stream/3`.
""".
-opaque stream_ref() :: reference().

-doc """
Facade error, including storage reasons.
""".
-type reason() ::
    book_storage:reason() |
    {already_started, pid()} |
    {unknown_metric, atom()} |
    {already_declared, atom()} |
    {kind_mismatch, atom(), kind()} |
    {labels_mismatch, atom(), term()} |
    {wrong_kind, atom(), kind()} |
    {negative_increment, number()} |
    {reserved_label, atom()} |
    {invalid_label, atom(), missing | extra | term()} |
    {invalid_option, term()} |
    {invalid_time, term()} |
    {too_many_lines, non_neg_integer()} |
    {unknown_stream, reference()}.

-doc """
Open a metrics book with facade defaults.

Default `max_pages` is 32 on memory and folder. The option is omitted on file.
""".
-spec open(target()) -> {ok, pid()} | {error, reason()}.
open(Target) ->
    open(Target, []).

-doc """
Open a metrics book.

`{name, Atom}` registers `{local, Atom}`. A taken name is
`{error, {already_started, Pid}}`.
""".
-spec open(target(), [option()]) -> {ok, pid()} | {error, reason()}.
open(Target, Options) ->
    {Reg, Rest} = take_name(Options),
    Start = case Reg of
        undefined ->
            gen_server:start_link(?MODULE, {Target, Rest}, []);
        Atom ->
            gen_server:start_link({local, Atom}, ?MODULE, {Target, Rest}, [])
    end,
    case Start of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {error, {already_started, Pid}};
        {error, Reason} ->
            {error, Reason}
    end.

-doc """
Stop the server and close the book.

It is idempotent: `ok` if the process is already dead.
""".
-spec close(server()) -> ok.
close(Server) ->
    try
        gen_server:stop(Server)
    catch
        exit:noproc ->
            ok;
        exit:{noproc, _} ->
            ok
    end.

-doc """
Return `book_storage:info/1` for the held book.
""".
-spec info(server()) -> {ok, book_storage:book_info()} | {error, reason()}.
info(Server) ->
    call(Server, info).

-doc """
Fsync the held book.
""".
-spec sync(server()) -> ok | {error, reason()}.
sync(Server) ->
    call(Server, sync).

-doc """
Declare a metric with no required labels.
""".
-spec declare(server(), name(), kind()) -> ok | {error, reason()}.
declare(Server, Name, Kind) ->
    declare(Server, Name, Kind, []).

-doc """
Declare a metric.

It writes nothing to the book. A matching second declare is `ok`. A different
kind is `{already_declared, Name}` in this process, or `{kind_mismatch, Name,
Stored}` after reopen. A different required-key set is `{labels_mismatch,
Name, Previous}`.
""".
-spec declare(server(), name(), kind(), [declare_option()]) ->
    ok | {error, reason()}.
declare(Server, Name, Kind, Opts) ->
    call(Server, {declare, Name, Kind, Opts}).

-doc """
Increment a counter by 1 with no labels.
""".
-spec inc(server(), name()) -> ok | {error, reason()}.
inc(Server, Name) ->
    inc(Server, Name, #{}, 1).

-doc """
Increment a counter by `Amount` with no labels.
""".
-spec inc(server(), name(), number()) -> ok | {error, reason()}.
inc(Server, Name, Amount) when is_number(Amount) ->
    inc(Server, Name, #{}, Amount).

-doc """
Increment a counter.

Time is `max(Now, LastTime)` inside the server so a backward clock cannot
lock the book. A successful call appends one sample with the new cumulative
value.
""".
-spec inc(server(), name(), labels(), number()) -> ok | {error, reason()}.
inc(Server, Name, Labels, Amount) ->
    call(Server, {inc, auto, Name, Labels, Amount}).

-doc """
Increment a counter at an explicit time.

It does not clamp. Out-of-order times surface `{out_of_order, Time, Last}`.
""".
-spec inc(server(), book_storage:time(), name(), labels(), number()) ->
    ok | {error, reason()}.
inc(Server, Time, Name, Labels, Amount) ->
    call(Server, {inc, {time, Time}, Name, Labels, Amount}).

-doc """
Set a gauge with no labels.
""".
-spec set(server(), name(), number()) -> ok | {error, reason()}.
set(Server, Name, Value) ->
    set(Server, Name, #{}, Value).

-doc """
Set a gauge.

Time is `max(Now, LastTime)` inside the server so a backward clock cannot
lock the book. A successful call appends one sample with the new value.
""".
-spec set(server(), name(), labels(), number()) -> ok | {error, reason()}.
set(Server, Name, Labels, Value) ->
    call(Server, {set, auto, Name, Labels, Value}).

-doc """
Set a gauge at an explicit time.

It does not clamp. Out-of-order times surface `{out_of_order, Time, Last}`.
""".
-spec set(server(), book_storage:time(), name(), labels(), number()) ->
    ok | {error, reason()}.
set(Server, Time, Name, Labels, Value) ->
    call(Server, {set, {time, Time}, Name, Labels, Value}).

-doc """
Current value of a series with no labels.
""".
-spec value(server(), name()) -> {ok, number() | undefined} | {error, reason()}.
value(Server, Name) ->
    value(Server, Name, #{}).

-doc """
Current value of a series.

It reads the in-memory cache, not the book, and requires declare. A missing
series is `{ok, undefined}`.
""".
-spec value(server(), name(), labels()) ->
    {ok, number() | undefined} | {error, reason()}.
value(Server, Name, Labels) ->
    call(Server, {value, Name, Labels}).

-doc """
Query samples, oldest-first.

It does not require declare. `query(#{name => http_requests})` is legal for a
labeled metric.
""".
-spec query(server(), query()) -> {ok, [sample()]} | {error, reason()}.
query(Server, Query) when is_map(Query) ->
    call(Server, {query, Query}).

-doc """
Start a stream.

The call replies `{ok, Ref}` before historical samples are delivered, then
the subscriber receives historical matches, `{metrics_book, Ref, live}`, and
later matching samples. `last => 0` is live-only. A map with no window
defaults to `last => 100`.
""".
-spec stream(server(), query(), pid()) -> {ok, stream_ref()} | {error, reason()}.
stream(Server, Query, Pid) when is_map(Query), is_pid(Pid) ->
    call(Server, {stream, Query, Pid}).

-doc """
Cancel a stream and send `{metrics_book, Ref, closed}`.
""".
-spec cancel(server(), stream_ref()) -> ok | {error, reason()}.
cancel(Server, Ref) ->
    call(Server, {cancel, Ref}).

call(Server, Req) ->
    gen_server:call(Server, Req, ?CALL_TIMEOUT).

-doc false.
init({Target, Options}) ->
    case parse_open_options(Options) of
        {error, Reason} ->
            {stop, Reason};
        {ok, MaxMailbox, BookOpts} ->
            BookOpts1 = apply_max_pages_default(Target, BookOpts, 32),
            case book_storage:open(Target, BookOpts1) of
                {ok, Book} ->
                    case restore(Book) of
                        {error, Reason} ->
                            _ = book_storage:close(Book),
                            {stop, Reason};
                        {ok, LastTime, RestoreValues, RestoreKinds} ->
                            {ok, #state{
                                book = Book,
                                last_time = LastTime,
                                max_mailbox = MaxMailbox,
                                restore_values = RestoreValues,
                                restore_kinds = RestoreKinds
                            }}
                    end;
                {error, Reason} ->
                    {stop, Reason}
            end
    end.

-doc false.
handle_call({declare, Name, Kind, Opts}, _From, State) ->
    {Reply, State2} = do_declare(Name, Kind, Opts, State),
    {reply, Reply, State2};
handle_call({inc, TimeSpec, Name, Labels, Amount}, _From, State) ->
    {Reply, State2} = do_update(counter, TimeSpec, Name, Labels, {inc, Amount}, State),
    {reply, Reply, State2};
handle_call({set, TimeSpec, Name, Labels, Value}, _From, State) ->
    {Reply, State2} = do_update(gauge, TimeSpec, Name, Labels, {set, Value}, State),
    {reply, Reply, State2};
handle_call({value, Name, Labels}, _From, State) ->
    {reply, do_value(Name, Labels, State), State};
handle_call({query, Query}, _From, #state{book = Book} = State) ->
    {reply, run_query(Book, Query), State};
handle_call({stream, Query, Pid}, From, State) ->
    start_stream(Query, Pid, From, State);
handle_call({cancel, Ref}, _From, State) ->
    case take_stream(Ref, State#state.streams) of
        {ok, Stream, Rest} ->
            send_overflow_then(Stream, closed),
            demonitor(Stream#stream.mon, [flush]),
            {reply, ok, State#state{streams = Rest}};
        error ->
            {reply, {error, {unknown_stream, Ref}}, State}
    end;
handle_call(info, _From, #state{book = Book} = State) ->
    {reply, book_storage:info(Book), State};
handle_call(sync, _From, #state{book = Book} = State) ->
    {reply, book_storage:sync(Book), State}.

-doc false.
handle_cast(_Msg, State) ->
    {noreply, State}.

-doc false.
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State) ->
    Streams = [S || S <- State#state.streams, S#stream.mon =/= Mon],
    {noreply, State#state{streams = Streams}};
handle_info(_Info, State) ->
    {noreply, State}.

-doc false.
terminate(_Reason, #state{book = Book, streams = Streams}) ->
    lists:foreach(
        fun(S) ->
            send_overflow_then(S, closed),
            demonitor(S#stream.mon, [flush])
        end,
        Streams
    ),
    _ = book_storage:close(Book),
    ok.

restore(Book) ->
    {ok, Info} = book_storage:info(Book),
    LastTime = maps:get(last_time, Info),
    LineCount = maps:get(line_count, Info),
    if
        LineCount > ?MAX_RESTORE ->
            {error, {too_many_lines, LineCount}};
        LineCount =:= 0 ->
            {ok, LastTime, #{}, #{}};
        true ->
            case book_storage:query(Book, #{from => beginning, to => latest, last => LineCount}) of
                {ok, Lines} ->
                    {Values, Kinds} = lists:foldl(fun fold_restore/2, {#{}, #{}}, Lines),
                    {ok, LastTime, Values, Kinds};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

fold_restore(Line, {Values, Kinds}) ->
    case decode_line(Line) of
        {ok, #{name := Name, kind := Kind, labels := Labels, value := Value}} ->
            {Values#{{Name, Labels} => Value}, Kinds#{Name => Kind}};
        skip ->
            {Values, Kinds}
    end.

do_declare(Name, Kind, Opts, State) when is_atom(Name), (Kind =:= counter orelse Kind =:= gauge) ->
    Keys = case lists:keyfind(labels, 1, Opts) of
        {labels, Ks} when is_list(Ks) ->
            lists:usort(Ks);
        false ->
            any;
        _ ->
            bad
    end,
    case Keys of
        bad ->
            {{error, {invalid_option, Opts}}, State};
        _ ->
            declare_keys(Name, Kind, Keys, State)
    end;
do_declare(Name, Kind, _Opts, State) ->
    {{error, {invalid_option, {Name, Kind}}}, State}.

declare_keys(Name, Kind, Keys, #state{declared = Decl} = State) ->
    case maps:find(Name, Decl) of
        {ok, {Kind, Keys}} ->
            {ok, State};
        {ok, {Kind, Prev}} ->
            {{error, {labels_mismatch, Name, Prev}}, State};
        {ok, {_OtherKind, _}} ->
            {{error, {already_declared, Name}}, State};
        error ->
            case maps:find(Name, State#state.restore_kinds) of
                {ok, Stored} when Stored =/= Kind ->
                    {{error, {kind_mismatch, Name, Stored}}, State};
                _ ->
                    Values = copy_restore(Name, State#state.restore_values, State#state.values),
                    {ok, State#state{
                        declared = Decl#{Name => {Kind, Keys}},
                        values = Values
                    }}
            end
    end.

copy_restore(Name, Restore, Values) ->
    maps:fold(
        fun
            ({N, L}, V, Acc) when N =:= Name ->
                Acc#{{N, L} => V};
            (_, _, Acc) ->
                Acc
        end,
        Values,
        Restore
    ).

do_value(Name, Labels, State) ->
    case maps:find(Name, State#state.declared) of
        error ->
            {error, {unknown_metric, Name}};
        {ok, {_Kind, Keys}} ->
            case canonicalize_labels(Labels, Keys) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Canon} ->
                    {ok, maps:get({Name, Canon}, State#state.values, undefined)}
            end
    end.

do_update(NeedKind, TimeSpec, Name, Labels, Op, State) ->
    case maps:find(Name, State#state.declared) of
        error ->
            {{error, {unknown_metric, Name}}, State};
        {ok, {Kind, _Keys}} when Kind =/= NeedKind ->
            {{error, {wrong_kind, Name, Kind}}, State};
        {ok, {Kind, Keys}} ->
            case canonicalize_labels(Labels, Keys) of
                {error, Reason} ->
                    {{error, Reason}, State};
                {ok, Canon} ->
                    case next_value(Op, maps:get({Name, Canon}, State#state.values, 0), Kind) of
                        {error, Reason} ->
                            {{error, Reason}, State};
                        {ok, NewVal} ->
                            write_sample(TimeSpec, Name, Kind, Canon, NewVal, State)
                    end
            end
    end.

next_value({inc, Amount}, _Cur, _Kind) when not is_number(Amount) ->
    {error, {invalid_option, Amount}};
next_value({inc, Amount}, _Cur, _Kind) when Amount < 0 ->
    {error, {negative_increment, Amount}};
next_value({inc, Amount}, Cur, counter) ->
    {ok, Cur + Amount};
next_value({set, Value}, _Cur, gauge) when is_number(Value) ->
    {ok, Value};
next_value({set, Value}, _Cur, gauge) ->
    {error, {invalid_option, Value}}.

write_sample(TimeSpec, Name, Kind, Canon, NewVal, State) ->
    Time = stamp(TimeSpec, State#state.last_time),
    case Time of
        {error, Reason} ->
            {{error, Reason}, State};
        T ->
            Sample = #{
                time => T,
                name => Name,
                kind => Kind,
                labels => Canon,
                value => NewVal
            },
            Value = encode_value(Kind, NewVal),
            Tags = encode_tags(Name, Kind, Canon),
            case book_storage:write(State#state.book, T, Value, Tags) of
                {ok, Book2} ->
                    State2 = State#state{
                        book = Book2,
                        last_time = T,
                        values = (State#state.values)#{{Name, Canon} => NewVal}
                    },
                    {ok, fanout(Sample, State2)};
                {error, Reason, Book3} ->
                    {{error, Reason}, State#state{book = Book3}}
            end
    end.

stamp(auto, empty) ->
    erlang:system_time(millisecond);
stamp(auto, Last) ->
    max(erlang:system_time(millisecond), Last);
stamp({time, T}, empty) when is_integer(T), T >= 0 ->
    T;
stamp({time, T}, Last) when is_integer(T), T >= 0 ->
    case T < Last of
        true ->
            {error, {out_of_order, T, Last}};
        false ->
            T
    end;
stamp({time, T}, _) ->
    {error, {invalid_time, T}}.

canonicalize_labels(Labels, Keys) when is_map(Labels) ->
    case maps:fold(fun fold_label/3, {ok, #{}}, Labels) of
        {error, Reason} ->
            {error, Reason};
        {ok, Canon} ->
            check_required(Canon, Keys)
    end;
canonicalize_labels(Other, _Keys) ->
    {error, {invalid_label, invalid, Other}}.

fold_label(name, _V, _Acc) ->
    {error, {reserved_label, name}};
fold_label(kind, _V, _Acc) ->
    {error, {reserved_label, kind}};
fold_label(K, V, {ok, Acc}) when is_atom(K) ->
    case canon_value(V) of
        {ok, Bin} ->
            {ok, Acc#{K => Bin}};
        error ->
            {error, {invalid_label, K, V}}
    end;
fold_label(K, V, {ok, _}) ->
    {error, {invalid_label, K, V}};
fold_label(_, _, {error, _} = Err) ->
    Err.

check_required(Canon, any) ->
    {ok, Canon};
check_required(Canon, Keys) ->
    Have = maps:keys(Canon),
    case {Have -- Keys, Keys -- Have} of
        {[], []} ->
            {ok, Canon};
        {[Extra | _], _} ->
            {error, {invalid_label, Extra, extra}};
        {[], [Missing | _]} ->
            {error, {invalid_label, Missing, missing}}
    end.

canon_value(V) when is_atom(V) ->
    {ok, atom_to_binary(V, utf8)};
canon_value(V) when is_integer(V) ->
    {ok, integer_to_binary(V)};
canon_value(V) ->
    case unicode:characters_to_binary(V) of
        Bin when is_binary(Bin) ->
            {ok, Bin};
        _ ->
            error
    end.

encode_value(Kind, Number) ->
    Term = term_to_binary(#{kind => Kind, value => Number}, [{minor_version, 1}]),
    <<?VERSION, Term/binary>>.

encode_tags(Name, Kind, Labels) ->
    Labels#{
        name => atom_to_binary(Name, utf8),
        kind => atom_to_binary(Kind, utf8)
    }.

run_query(Book, Query) ->
    case to_book_query(Query, query) of
        {error, Reason} ->
            {error, Reason};
        {ok, BQ} ->
            case book_storage:query(Book, BQ) of
                {ok, Lines} ->
                    {ok, [S || Line <- Lines, {ok, S} <- [decode_line(Line)]]};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

to_book_query(Query, Kind) ->
    case query_labels(Query) of
        {error, Reason} ->
            {error, Reason};
        {ok, Labels} ->
            Tags = case maps:find(name, Query) of
                {ok, Name} when is_atom(Name) ->
                    Labels#{name => atom_to_binary(Name, utf8)};
                error ->
                    Labels;
                {ok, Other} ->
                    {error, {invalid_option, {name, Other}}}
            end,
            case Tags of
                {error, Reason} ->
                    {error, Reason};
                _ ->
                    BQ = maps:with([from, to, last, limit], Query),
                    BQ1 = case {Kind, has_window(Query)} of
                        {stream, false} ->
                            BQ#{last => maps:get(last, Query, 100)};
                        _ ->
                            BQ
                    end,
                    {ok, BQ1#{tags => Tags}}
            end
    end.

has_window(Query) ->
    maps:is_key(from, Query) orelse maps:is_key(to, Query) orelse maps:is_key(last, Query).

query_labels(Query) ->
    case maps:get(labels, Query, #{}) of
        Map when is_map(Map) ->
            canonicalize_labels(Map, any);
        Other ->
            {error, {invalid_label, invalid, Other}}
    end.

decode_line(#{time := Time, value := <<?VERSION, Term/binary>>, tags := Tags}) ->
    try binary_to_term(Term) of
        #{kind := Kind, value := Value} ->
            NameBin = maps:get(name, Tags, <<"unknown">>),
            Name = binary_to_atom(NameBin, utf8),
            Labels = maps:without([name, kind], Tags),
            {ok, #{
                time => Time,
                name => Name,
                kind => Kind,
                labels => Labels,
                value => Value
            }}
    catch
        _:_ ->
            skip
    end;
decode_line(_) ->
    skip.

start_stream(Query, Pid, From, #state{book = Book, last_time = Last, max_mailbox = Max} = State) ->
    case to_book_query(Query, stream) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, BQ} ->
            case book_storage:query(Book, BQ) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                {ok, Lines} ->
                    Samples = [S || Line <- Lines, {ok, S} <- [decode_line(Line)]],
                    Ref = make_ref(),
                    Mon = monitor(process, Pid),
                    Name = maps:get(name, Query, undefined),
                    {ok, Labels} = query_labels(Query),
                    Silent = is_silent(Query, Last),
                    Stream = #stream{
                        ref = Ref,
                        pid = Pid,
                        mon = Mon,
                        name = Name,
                        labels = Labels,
                        silent = Silent
                    },
                    gen_server:reply(From, {ok, Ref}),
                    send_history(Pid, Ref, Samples, Max),
                    {noreply, State#state{streams = [Stream | State#state.streams]}}
            end
    end.

is_silent(Query, LastTime) ->
    case maps:get(to, Query, latest) of
        latest ->
            false;
        To when is_integer(To) ->
            Now = erlang:system_time(millisecond),
            Next = case LastTime of
                empty ->
                    Now;
                Last ->
                    max(Now, Last)
            end,
            To < Next;
        _ ->
            false
    end.

send_history(Pid, Ref, Samples, Max) ->
    Dropped = send_each(Pid, Ref, Samples, Max, 0),
    case Dropped of
        0 ->
            ok;
        N ->
            Pid ! {metrics_book, Ref, {overflow, N}}
    end,
    Pid ! {metrics_book, Ref, live},
    ok.

send_each(_Pid, _Ref, [], _Max, Dropped) ->
    Dropped;
send_each(Pid, Ref, [S | Rest], Max, Dropped) ->
    case mailbox_len(Pid) > Max of
        true ->
            send_each(Pid, Ref, Rest, Max, Dropped + 1);
        false ->
            Pid ! {metrics_book, Ref, {sample, S}},
            send_each(Pid, Ref, Rest, Max, Dropped)
    end.

fanout(Sample, #state{streams = Streams, max_mailbox = Max} = State) ->
    State#state{streams = [fanout_one(S, Sample, Max) || S <- Streams]}.

fanout_one(#stream{silent = true} = S, _Sample, _Max) ->
    S;
fanout_one(#stream{pid = Pid, ref = Ref, overflow = Over} = S, Sample, Max) ->
    case stream_match(S, Sample) of
        false ->
            S;
        true ->
            case mailbox_len(Pid) > Max of
                true ->
                    S#stream{overflow = Over + 1};
                false ->
                    case Over of
                        0 ->
                            ok;
                        N ->
                            Pid ! {metrics_book, Ref, {overflow, N}}
                    end,
                    Pid ! {metrics_book, Ref, {sample, Sample}},
                    S#stream{overflow = 0}
            end
    end.

stream_match(#stream{name = WantName, labels = Want}, #{name := Name, labels := Have}) ->
    NameOk = WantName =:= undefined orelse WantName =:= Name,
    LabelsOk = maps:fold(
        fun(K, V, Acc) ->
            Acc andalso maps:get(K, Have, undefined) =:= V
        end,
        true,
        Want
    ),
    NameOk andalso LabelsOk.

send_overflow_then(#stream{pid = Pid, ref = Ref, overflow = Over}, closed) ->
    case Over of
        0 ->
            ok;
        N ->
            Pid ! {metrics_book, Ref, {overflow, N}}
    end,
    Pid ! {metrics_book, Ref, closed},
    ok.

take_stream(Ref, Streams) ->
    case lists:keytake(Ref, #stream.ref, Streams) of
        {value, Stream, Rest} ->
            {ok, Stream, Rest};
        false ->
            error
    end.

mailbox_len(Pid) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, N} ->
            N;
        undefined ->
            0
    end.

take_name(Options) ->
    case lists:keytake(name, 1, Options) of
        {value, {name, Atom}, Rest} when is_atom(Atom) ->
            {Atom, Rest};
        false ->
            {undefined, Options}
    end.

parse_open_options(Options) ->
    parse_open_options(Options, 1000, []).

parse_open_options([], MaxMailbox, BookOpts) ->
    {ok, MaxMailbox, lists:reverse(BookOpts)};
parse_open_options([{timeout, _} | Rest], MaxMailbox, BookOpts) ->
    parse_open_options(Rest, MaxMailbox, BookOpts);
parse_open_options([{max_mailbox, N} | Rest], _Max, BookOpts) when is_integer(N), N > 0 ->
    parse_open_options(Rest, N, BookOpts);
parse_open_options([Opt | Rest], MaxMailbox, BookOpts) ->
    parse_open_options(Rest, MaxMailbox, [Opt | BookOpts]).

apply_max_pages_default({file, _}, BookOpts, _Default) ->
    BookOpts;
apply_max_pages_default(_Target, BookOpts, Default) ->
    case proplists:is_defined(max_pages, BookOpts) of
        true ->
            BookOpts;
        false ->
            [{max_pages, Default} | BookOpts]
    end.
