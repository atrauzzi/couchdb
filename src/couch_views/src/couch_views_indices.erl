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

-module(couch_views_indices).


-callback build_indices(Db :: map(), DDocs :: list()) -> [ok | {error, term()}].


-export([
    register_index/1,
    build_indices/2
]).


register_index(Mod) when is_atom(Mod) ->
    Indices = lists:usort([Mod | registrations()]),
    application:set_env(couch_views, indices, Indices).


build_indices(Db, DDocs) ->
    lists:flatmap(fun(Mod) ->
        Mod:build_indices(Db, DDocs)
    end, registrations()).


registrations() ->
    application:get_env(couch_views, indices, []).
