% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_views_updater).


-behaviour(gen_server).


-export([
    db_updated/1,

    start_link/0,

    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-include_lib("couch/include/couch_db.hrl").


-define(SHARDS, 32).
-define(DEFAULT_DELAY_MSEC, 60000).
-define(DEFAULT_RESOLUTION_MSEC, 5000).


db_updated(#{} = Db) ->
    DbName = fabric2_db:name(Db),
    Table = table(erlang:phash2(Db) rem ?SHARDS),
    NowMsec = now_msec(),
    ets:insert_new(Table, {DbName, NowMsec}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    lists:foreach(fun(T) ->
        spawn_link(fun() -> process_loop(T) end)
    end, create_tables()),
    {ok, nil}.


terminate(_, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


now_msec() ->
    erlang:monotonic_time(millisecond).


process_loop(Table) ->
    Now = now_msec(),
    Delay = delay_msec(),
    Since = Now - Delay,
    process_updates(Table, Since),
    clean_stale(Table, Since),
    Resolution = resolution_msec(),
    Jitter = rand:uniform(1 + trunc(Resolution * 0.25)),
    timer:sleep(Resolution + Jitter),
    process_loop(Table).


clean_stale(Table, Since) ->
    Head = {'_', '$1'},
    Guard = {'<', '$1', Since},
    % Monotonic is not strictly monotonic, process items using `=<` but clean
    % with `<` in case there was an update with the same time as NowMsec after
    % we started processing processed update in that cycle.
    ets:select_delete(Table, [{Head, [Guard], [true]}]).


process_updates(Table, Since) ->
    Head = {'$1', '$2'},
    Guard = {'=<', '$2', Since},
    case ets:select(Table, [{Head, [Guard], ['$1']}], 25) of
        '$end_of_table' -> ok;
        {Match, Cont} -> process_updates_iter(Match, Cont)
    end.


process_updates_iter([], Cont) ->
    case ets:select(Cont) of
        '$end_of_table' -> ok;
        {Match, Cont1} -> process_updates_iter(Match, Cont1)
    end;


process_updates_iter([Db | Rest], Cont) ->
    try
        process_db(Db)
    catch
        error:database_does_not_exist ->
            ok;
        Tag:Reason ->
            LogMsg = "~p failed to build indices for `~s` ~p:~p",
            couch_log:error(LogMsg, [?MODULE, Db, Tag, Reason])
    end,
    process_updates_iter(Rest, Cont).


delay_msec() ->
    config:get_integer("couch_views", "updater_delay_msec",
        ?DEFAULT_DELAY_MSEC).


resolution_msec() ->
    config:get_integer("couch_views", "updater_resolution_msec",
        ?DEFAULT_RESOLUTION_MSEC).


create_tables() ->
    Opts = [
        named_table,
        public,
        {write_concurency, true},
        {read_concurrency, true}
    ],
    Tables = [table(N) || N <- lists:seq(0, ?SHARDS - 1)],
    [ets:new(T, Opts) || T <- Tables].


table(Id) when is_integer(Id), Id >= 0 andalso Id < ?SHARDS ->
    list_to_atom("couch_views_updater_" ++ integer_to_list(Id)).


process_db(DbName) when is_binary(DbName) ->
    {ok, Db} = fabric2_db:open(DbName, [?ADMIN_CTX]),
    fabric2_db:transactional(Db, fun(TxDb) ->
        DDocs = get_design_docs(TxDb),
        couch_views_indices:build_indices(TxDb, shuffle(DDocs))
    end).

get_design_docs(Db) ->
    Callback = fun
        ({meta, _}, Acc) ->  {ok, Acc};
        (complete, Acc) ->  {ok, Acc};
        ({row, Row}, Acc) -> {ok, acc_doc(Db, Row, Acc)}
    end,
    {ok, DDocs} = fabric2_db:fold_design_docs(Db, Callback, [], []),
    DDocs.


acc_doc(Db, Row, Acc) ->
    {_, DocId} = lists:keyfind(id, 1, Row),
    case fabric2_db:open_doc(Db, DocId, []) of
        {not_found, missing} ->  Acc;
        {ok, #doc{deleted = true}} -> Acc;
        {ok, #doc{} = Doc} -> [couch_doc:to_json_obj(Doc) | Acc]
    end.


shuffle(DDocs) ->
    [D || {_, D} <- lists:sort([{random:uniform(), D} || D <- DDocs])].
