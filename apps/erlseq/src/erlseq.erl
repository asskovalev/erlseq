-module(erlseq).
-include_lib("eunit/include/eunit.hrl").
-export([create/1, map/2, filter/2, join/5, group/2]).
-export([count/1, count/2, 
		 first/1, first/2, 
		 single/1, single/2, 
		 sort/1, sort/2, sort_desc/1, sort_desc/2,
		 take/2, skip/2,
		 iter/2,
		 fold/2, fold/3, fold_right/3,
		 at/2,
		 reverse/1,
		 process/1,
		 to_list/1]).

-export([avg/1, sum/1,
		 all/1, all/2, 
		 any/1, any/2]).

-record(?MODULE, {
	type,
	ops,
	data
}).

%%%%%%%%%%%
% INTERFACE
%%%%%%%%%%%
create(L) when is_list(L) ->
	#?MODULE{type=list, ops=[], data=L}.



% mapi
map(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{map, Fn}]}.



% iteri
iter(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{iter, Fn}]}.



% filteri
filter(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{filter, Fn}]}.



join(#?MODULE{}=S2, FnS1, FnS2, FnS, #?MODULE{ops=Ops}=S) when is_function(FnS1), is_function(FnS2), is_function(FnS) ->
	S#?MODULE{ops=Ops++[{join, S2, FnS1, FnS2, FnS}]};

join(S2, FnS1, FnS2, FnS, #?MODULE{}=S) when is_list(S2), is_function(FnS1), is_function(FnS2), is_function(FnS) ->
	S:join(?MODULE:create(S2), FnS1, FnS2, FnS).



sort(#?MODULE{ops=Ops}=S) ->
	S#?MODULE{ops=Ops++[{sort, asc, undefined}]}.

sort(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{sort, asc, Fn}]}.

sort_desc(#?MODULE{ops=Ops}=S) ->
	S#?MODULE{ops=Ops++[{sort, desc,undefined}]}.

sort_desc(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{sort, desc, Fn}]}.



take(N, #?MODULE{ops=Ops}=S) when is_integer(N) ->
	S#?MODULE{ops=Ops++[{take, N}]}.

skip(N, #?MODULE{ops=Ops}=S) when is_integer(N) ->
	S#?MODULE{ops=Ops++[{skip, N}]}.



reverse(#?MODULE{ops=Ops}=S) ->
	S#?MODULE{ops=Ops++[reverse]}.


group(Fn, #?MODULE{ops=Ops}=S) when is_function(Fn) ->
	S#?MODULE{ops=Ops++[{group, Fn}]}.


% fold(Fn, #?MODULE{ops=Ops}=S)
% fold(Fn, Zero, #?MODULE{ops=Ops}=S)

% last(#?MODULE{ops=Ops}=S)
% last(Fn, #?MODULE{ops=Ops}=S)

% zip(Seq, #?MODULE{ops=Ops}=S)

% take_while(Fn, #?MODULE{ops=Ops}=S)
% drop_while(Fn, #?MODULE{ops=Ops}=S)

% split_by(N, #?MODULE{ops=Ops}=S)
% split_to(N, #?MODULE{ops=Ops}=S)


%%%%%%%%%%%%%%%%%
% MATERIALIZATION
%%%%%%%%%%%%%%%%%

process(#?MODULE{type=list}=S) ->
	transform_list(S).

at(N, #?MODULE{type=list}=S) when is_integer(N), N >=0 ->
	case transform_list(S) of
		#?MODULE{data=Data} when length(Data) > N -> lists:nth(N+1, Data);
		#?MODULE{} -> {error, out_of_range}
	end.

first(Fn, #?MODULE{type=list}=S0) ->
	S = S0:filter(Fn),
	S:first().

first(#?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=[]} -> {error, empty};
		#?MODULE{data=[Hd|_]} -> Hd
	end.


single(Fn, #?MODULE{type=list}=S0) ->
	S = S0:filter(Fn),
	S:single().

single(#?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=[Elem]} -> Elem;
		#?MODULE{data=Data} when length(Data) > 1 -> {error, multiple}
	end.


count(Fn, #?MODULE{type=list}=S0) ->
	S = S0:filter(Fn),
	S:count().

count(#?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} when is_list(Data) -> length(Data);
		#?MODULE{} -> {error, not_list}
	end.


to_list(#?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} -> Data
	end.


fold(Fn, #?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=[]} -> {error, empty};
		#?MODULE{data=[Zero]} -> Zero;
		#?MODULE{data=[Zero|Rest]} -> lists:foldl(Fn, Zero, Rest)
	end.

fold(Fn, Zero, #?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} -> lists:foldl(Fn, Zero, Data)
	end.

fold_right(Fn, Zero, #?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} -> lists:foldr(Fn, Zero, Data)
	end.


sum(#?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} -> lists:sum(Data)
	end.

avg(#?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} -> lists:sum(Data) / length(Data)
	end.

all(#?MODULE{type=list}=S) ->
	all(fun(X) -> X end, S).
all(Fn, #?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} -> lists:all(Fn, Data)
	end.

any(#?MODULE{type=list}=S) ->
	any(fun(X) -> X end, S).
any(Fn, #?MODULE{type=list}=S) ->
	case transform_list(S) of
		#?MODULE{data=Data} -> lists:any(Fn, Data)
	end.


%%%%%%%%%%%%%%%%
% TRANSFORMATION
%%%%%%%%%%%%%%%%
transform_list(#?MODULE{type=list, ops=[]}=S) -> 
	S;


transform_list(#?MODULE{type=list, ops=[{iter, Fn}|Rest], data=In}=S) ->
	lists:foreach(Fn, In),
	transform_list(S#?MODULE{ops=Rest});


transform_list(#?MODULE{type=list, ops=[reverse|Rest], data=In}=S) ->
	Out = lists:reverse(In),
	transform_list(S#?MODULE{data=Out, ops=Rest});


transform_list(#?MODULE{type=list, ops=[{map, Fn}|Rest], data=In}=S) ->
	Out = lists:map(Fn, In),
	transform_list(S#?MODULE{data=Out, ops=Rest});


transform_list(#?MODULE{type=list, ops=[{filter, Fn}|Rest], data=In}=S) ->
	Out = lists:filter(Fn, In),
	transform_list(S#?MODULE{data=Out, ops=Rest});


transform_list(#?MODULE{type=list, ops=[{take, N}|Rest], data=In}=S) ->
	Out = lists:sublist(In, N),
	transform_list(S#?MODULE{data=Out, ops=Rest});


transform_list(#?MODULE{type=list, ops=[{skip, N}|Rest], data=In}=S) ->
	Out = 
		case length(In) of
			Len when N >= Len -> [];
			_ -> lists:nthtail(N, In)
		end,
	transform_list(S#?MODULE{data=Out, ops=Rest});


transform_list(#?MODULE{type=list, ops=[{group, FnKey}|Rest], data=In}=S) ->
	SDict = lists:foldl(fun(SElem, Acc) ->
		dict:update(FnKey(SElem), fun(Elems) -> [SElem|Elems] end, [SElem], Acc)
	end, dict:new(), In),
	SDict1 = dict:map(fun(_Key, Value) -> lists:reverse(Value) end, SDict),
	Out = dict:to_list(SDict1),
	transform_list(S#?MODULE{data=Out, ops=[reverse|Rest]});


transform_list(#?MODULE{type=list, ops=[{sort, Direction, FnKey}|Rest], data=In}=S) ->
	Out = 
		case FnKey of
			undefined -> 
				lists:sort(In);
			_ when is_function(FnKey) -> 
				lists:sort(fun(A, B) -> FnKey(A) =< FnKey(B) end, In)
		end,
	Reverse = case Direction of asc -> []; desc -> [reverse] end,
	transform_list(S#?MODULE{data=Out, ops=Reverse++Rest});


transform_list(#?MODULE{type=list, ops=[{join, #?MODULE{}=S2, FnS1, FnS2, FnS}|Rest], data=In1}=S1) ->
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
	transform_list(S1#?MODULE{data=Out, ops=Rest}).



%%%%%%%
% TESTS
%%%%%%%
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


reverse_test() ->
	S1 = erlseq:create([1,2,3,4]),
	S2 = S1:reverse(),
	[4,3,2,1] = S2:to_list().


sort_test() ->
	S1 = erlseq:create([4,2,3,1]),
	[1,2,3,4] = (S1:sort()):to_list(),
	[4,3,2,1] = (S1:sort_desc()):to_list(),

	S2 = erlseq:create([{3, 10}, {4, 0}, {1, 40}, {2, 40}]),
	S3 = S2:sort(fun ({A, _}) -> A end),
	[{1, 40}, {2, 40}, {3, 10}, {4, 0}] = S3:to_list(),

	S4 = S2:sort(fun ({_, B}) -> B end),
	[{4, 0}, {3, 10}, {1, 40}, {2, 40}] = S4:to_list().


map_filter_test() ->
	S1 = erlseq:create([1,2,3,4]),
	S2 = S1:filter(fun(X) -> X rem 2 == 0 end),
	S3 = S2:map(fun(X) -> X - 1 end),
	[1, 3] = S3:to_list().


skip_take_test() ->
	S1 = erlseq:create(lists:seq(1, 10)),
	S2 = S1:skip(2),
	S3 = S2:take(3),
	[3,4,5] = S3:to_list(),

	S4 = S3:take(10),
	[3,4,5] = S4:to_list(),

	S5 = S1:skip(20),
	[] = S5:to_list().


iter_test() ->
	Self = self(),
	S1 = erlseq:create([1,2,3,4]),
	S2 = S1:iter(fun(X) -> Self ! {map, X} end),

	[1,2,3,4] = S2:to_list(),
	[{map, 1}, {map, 2}, {map, 3}, {map, 4}] = collect_messages().



process_test() ->
	Self = self(),
	S1 = erlseq:create([1,2,3,4]),
	S2 = S1:iter(fun(X) -> Self ! {map, X} end),

	[1,2,3,4] = S2:to_list(),
	[{map, 1}, {map, 2}, {map, 3}, {map, 4}] = collect_messages(),

	3 = S2:at(2),
	[{map, 1}, {map, 2}, {map, 3}, {map, 4}] = collect_messages(),

	S3 = S2:process(),
	[{map, 1}, {map, 2}, {map, 3}, {map, 4}] = collect_messages(),

	[1,2,3,4] = S3:to_list(),
	[] = collect_messages(),

	3 = S3:at(2),
	[] = collect_messages().


scalar_test() ->
	S1 = erlseq:create(lists:seq(1, 10)),
	10 = S1:count(),

	1 = S1:at(0),
	7 = S1:at(6),
	{error, out_of_range} = S1:at(20),

	S2 = S1:filter(fun(X) -> X > 4 end),
	5 = S2:first(),
	6 = S2:first(fun(X) -> X rem 2 == 0 end),

	8 = S2:single(fun(X) -> X rem 4 == 0 end),
	{error, multiple} = S2:single(),
	{error, multiple} = S2:single(fun(X) -> X rem 3 == 0 end).


sum_avg_test() ->
	S1 = erlseq:create(lists:seq(1, 10)),
	55 = S1:sum(),
	5.5 = S1:avg().


all_any_test() ->
	S1 = erlseq:create(lists:seq(1, 3)),

	true  = S1:all(fun(X) -> X < 10 end),
	false = S1:all(fun(X) -> X < 2 end),
	true  = S1:any(fun(X) -> X < 2 end),

	S2 = S1:map(fun(X) -> X < 2 end),
	false = S2:all(),
	true = S2:any().



collect_messages() ->
	collect_messages([]).
collect_messages(Acc) ->
	receive
		M -> collect_messages([M|Acc])
	after 0 -> 
		lists:reverse(Acc)
	end.
