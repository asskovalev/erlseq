-module(erlseq).
-include_lib("eunit/include/eunit.hrl").
-export([create/1, map/2, filter/2, join/5, group/2, to_list/1]).

-record(?MODULE, {
	type,
	ops,
	data
}).


create(L) when is_list(L) ->
	#?MODULE{type=list, ops=[], data=L}.

map(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{map, Fn}]}.

filter(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{filter, Fn}]}.

join(#?MODULE{}=S2, FnS1, FnS2, FnS, #?MODULE{ops=Ops}=S) when is_function(FnS1), is_function(FnS2), is_function(FnS) ->
	S#?MODULE{ops=Ops++[{join, S2, FnS1, FnS2, FnS}]};

join(S2, FnS1, FnS2, FnS, #?MODULE{}=S) when is_list(S2), is_function(FnS1), is_function(FnS2), is_function(FnS) ->
	S:join(?MODULE:create(S2), FnS1, FnS2, FnS).

group(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{group, Fn}]}.

%%%%%%%%%%%%%%%%%
% MATERIALIZATION
%%%%%%%%%%%%%%%%%
to_list(#?MODULE{type=list, ops=[], data=In}) ->
	In;

to_list(#?MODULE{type=list, ops=[{group, FnKey}|Rest], data=In}=S) ->
	SDict = lists:foldl(fun(SElem, Acc) ->
		dict:update(FnKey(SElem), fun(Elems) -> io:format("upd ~p, ~p~n", [FnKey(SElem), Elems]), [SElem|Elems] end, [], Acc)
	end, dict:new(), In),
	SDict1 = dict:map(fun(_Key, Value) -> lists:reverse(Value) end, SDict),
	Out = dict:to_list(SDict1),
	to_list(S#?MODULE{data=Out, ops=Rest});

to_list(#?MODULE{type=list, ops=[{join, #?MODULE{}=S2, FnS1, FnS2, FnS}|Rest], data=In1}=S1) ->
	In2 = S2:to_list(),
	S2Dict = lists:foldl(fun (S2Elem, Acc) ->
		dict:store(FnS2(S2Elem), S2Elem, Acc)
	end, dict:new(), In2),
	Out = 
		lists:filtermap(fun(S1Elem) ->
			case dict:find(FnS1(S1Elem), S2Dict) of
				{ok, S2Elem} -> {true, FnS(S1Elem, S2Elem)};
				_ -> false
			end
		end, In1),
	to_list(S1#?MODULE{data=Out, ops=Rest});

to_list(#?MODULE{type=list, ops=[{Op, F}|Rest], data=In}=S) ->
	Out = erlang:apply(lists, Op, [F, In]),
	to_list(S#?MODULE{data=Out, ops=Rest}).


%%%%%%
% TEST
%%%%%%
map_test() ->
	S1 = erlseq:create([1,2,3,4]),
	S2 = S1:map(fun(X) -> X - 1 end),
	[0, 1, 2, 3] = S2:to_list().

filter_test() ->
	S1 = erlseq:create([1,2,3,4]),
	S2 = S1:filter(fun(X) -> X rem 2 == 0 end),
	[2, 4] = S2:to_list().

join_test() ->
	S1 = erlseq:create([{1, a}, {2, b}, {3, c}, {4, d}]),
	L1 = erlseq:create([{3, 10}, {4, 0}, {1, 40}, {2, 40}]),
	S2 = S1:join(
		L1,
		fun({Id, _Name}=_S) -> Id end,
		fun({Id, _Count}=_L) -> Id end,
		fun({Id, Name}, {Id, Count}) -> {Name, Count} end),
	[{a, 40}, 
     {b, 40}, 
     {c, 10}, 
     {d, 0}] = S2:to_list().

group_test() ->
	S1 = erlseq:create([{1, a}, {1, b}, {2, c}, {2, d}, {3, e}]),
	S2 = S1:group(fun({Key, _Value}) -> Key end),
	[{1, [{1, a}, {1, b}]}, 
	 {2, [{2, c}, {2, d}]}, 
	 {3, [{3, e}]}] = S2:to_list().

map_filter_test() ->
	S1 = erlseq:create([1,2,3,4]),
	S2 = S1:filter(fun(X) -> X rem 2 == 0 end),
	S3 = S2:map(fun(X) -> X - 1 end),
	[1, 3] = S3:to_list().

