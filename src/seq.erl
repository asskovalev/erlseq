-module(seq).
-export([create/1, map/2, filter/2, to_list/1]).
-record(seq, {
	type,
	ops,
	data
}).


create(L) when is_list(L) ->
	#seq{type=list, ops=[], data=L}.

map(Fn, #seq{ops=Ops}=S) when is_function(Fn) ->
	S#seq{ops=Ops++[{map, Fn}]}.

filter(Fn, #seq{ops=Ops}=S) when is_function(Fn) ->
	S#seq{ops=Ops++[{filter, Fn}]}.

to_list(#seq{type=list, ops=Ops, data=In}) ->
	lists:foldl(fun
		({Op, F}, L) ->
			erlang:apply(lists, Op, [F, L])
	end, In, Ops).