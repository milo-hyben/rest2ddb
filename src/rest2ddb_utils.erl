%% @author milo hyben
%% @doc REST to DynamoDB interface - utility module.


-module(rest2ddb_utils).

%% ====================================================================
%% API functions
%% ====================================================================

-export([
		get_resource/2,
		put_resource/3,
		post_resource/3,
		patch_resource/3,
		delete_resource/2,
		match_resource_to_keys/3
		]).

get_resource(Resource, Parameters) ->
	MatchedKeys = match_resource_to_keys(Resource, Parameters, []),
	get_record(MatchedKeys, Parameters).

put_resource(Resource, Parameters, Body) ->
	{Attributes, ConvertedBody} = extract_from_json(string:tokens(binary:bin_to_list(Body),":,{}][\""), 'put', Parameters, []),
	MatchedKeys = match_resource_to_keys(Resource, Attributes, []),
	put_record(MatchedKeys, Parameters, ConvertedBody).

post_resource(Resource, Parameters, Body) ->
	{Attributes, ConvertedBody} = extract_from_json(string:tokens(binary:bin_to_list(Body),":,{}][\""), 'none', Parameters, []),
	MatchedKeys = match_resource_to_keys(Resource, Attributes, []),
	post_record(MatchedKeys, Parameters, ConvertedBody).

patch_resource(Resource, Parameters, Body) ->
	{Attributes, ConvertedBody} = extract_from_json(string:tokens(binary:bin_to_list(Body),":,{}][\""), 'put', Parameters, []),
	MatchedKeys = match_resource_to_keys(Resource, Attributes, []),
	patch_record(MatchedKeys, Parameters, ConvertedBody).

delete_resource(Resource, Parameters) ->
	MatchedKeys = match_resource_to_keys(Resource, Parameters, []),
	delete_record(MatchedKeys, Parameters).

match_resource_to_keys([], _Parameters, Acc) -> Acc;
match_resource_to_keys([Resource, Id | Next], Parameters, Acc) ->
	case is_resource_a_table(Resource) of
		true -> extract_resource_keys(extract_table(Resource, Acc), [Id | Next], Parameters, Acc);
		_ -> {error,[]}  %% TODO check is Resource is not a column
	end;
match_resource_to_keys([Resource], Parameters, Acc) ->
	case is_resource_a_table(Resource) of
		true -> extract_resource_keys(extract_table(Resource, Acc), Parameters, Acc);
		_ -> {error,[]}  %% TODO check is Resource is not a column
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% get_record
get_record([], _) -> {error, []};
get_record([{TableName, KeyValues} | _T], Parameters) ->
	BinTableName = binary:list_to_bin(TableName),
	Keys = table_keys(BinTableName),
	case length(Keys) == length(KeyValues) of
		true  ->
				%% we have enough info to use get/find
				get_item('get', BinTableName, Keys, convert_to_bin(KeyValues, []), Parameters);
		false ->
				%scan
				get_item('scan', BinTableName, Keys, convert_to_bin(KeyValues, []), Parameters)
	end.

%% put_record
put_record([], _, _) -> {error, []};
put_record([{TableName, KeyValues} | _T], Parameters, Attributes) ->
	BinTableName = binary:list_to_bin(TableName),
	Keys = table_keys(BinTableName),
	case length(Keys) == length(KeyValues) of
		true  ->
				%% we have enough info to use get/find
				put_item(BinTableName, Keys, convert_to_bin(KeyValues, []), Attributes);
		false ->
				% item could not be located as there is not enough information about the primary key,
				% use post_item, basicaly create a new item
				post_item(BinTableName, Attributes)
	end.

%% post_record
post_record	([], _, _) -> {error, []};
post_record	([{TableName, KeyValues} | _T], Parameters, Attributes) ->
	BinTableName = binary:list_to_bin(TableName),
	post_item(BinTableName, Attributes).

%% patch_record
patch_record([], _, _) -> {error, []};
patch_record([{TableName, KeyValues} | _T], Parameters, Attributes) ->
	BinTableName = binary:list_to_bin(TableName),
	Keys = table_keys(BinTableName),
	case length(Keys) == length(KeyValues) of
		true  ->
				%% we have enough info to use get/find
				put_item(BinTableName, Keys, convert_to_bin(KeyValues, []), Attributes);
		false ->
				% item could not be located as there is not enough information about the primary key
				{error, []} %% TODO
	end.

%% delete_record
delete_record([], _) -> {error, []};
delete_record([{TableName, KeyValues} | _T], Parameters) ->
	BinTableName = binary:list_to_bin(TableName),
	Keys = table_keys(BinTableName),
	case length(Keys) == length(KeyValues) of
		true  ->
				%% we have enough info to use get/find
				delete_item(BinTableName, Keys, convert_to_bin(KeyValues, []), Parameters);
		false ->
				% item could not be located as there is not enough information about the primary key
				{error, []} %% TODO
	end.


pattern_match_keys(_Pattern, [], Result) -> Result;
pattern_match_keys(Pattern, [{K, V} | T], {Max, MaxLen, MaxPattern, MaxResult}) ->
	case re:run(K,Pattern,[global,{capture,[1],list}]) of
 		{match,R} ->
			case ((length(R) > Max) or ((length(R) == Max) and (length(lists:flatten(R)) > MaxLen))) of
				true ->
					pattern_match_keys(Pattern, T, {length(R), length(lists:flatten(R)), Pattern, {K, V}});
				_ ->
					pattern_match_keys(Pattern, T, {Max, MaxLen, MaxPattern, MaxResult})
	 		end;
 		_ -> pattern_match_keys(Pattern, T, {Max, MaxLen, MaxPattern, MaxResult})
	end.

pattern_match_keys(Pattern, K) ->
	pattern_match_keys(Pattern, K, {0, 0, [], []}).


concat_parameters_key_value([], Acc) -> Acc;
concat_parameters_key_value([{K,V} | T], Acc) ->
	case is_binary(K) of true -> KS = binary:bin_to_list(K); _ -> KS = K end,
	case is_binary(V) of true -> VS = binary:bin_to_list(V); _ -> VS = V end,
	concat_parameters_key_value(T, [{KS, VS}] ++ Acc).

concat_parameters_table_keys([], Acc) -> Acc;
concat_parameters_table_keys([{_Table, Keys} | T], Acc) ->
	concat_parameters_table_keys(T, concat_parameters_key_value(Keys, Acc)).

concat_parameters_incl_table_keys([], Acc) -> Acc;
concat_parameters_incl_table_keys([{Table, Keys} | T], Acc) ->
	MergedKeys = lists:map(fun({K,V}) -> {Table++"|"++K, V} end, Keys),
	concat_parameters_incl_table_keys(T, concat_parameters_key_value(MergedKeys, Acc)).


match_hashkey([{H, Pattern} | T], CombinedParams) ->
	MatchedParams = pattern_match_keys(Pattern, CombinedParams),
	case MatchedParams of
		{0,0,[],[]} -> match_hashkey(T, CombinedParams);
		_ -> {H, MatchedParams}
	end;

match_hashkey(_, _) -> {'none',{0, 0, [], []}}.


%%
create_a_regex_pattern([], Acc) -> lists:concat(["(", lists:concat(Acc), ")"]);
create_a_regex_pattern([H | T], []) ->
	create_a_regex_pattern(T, [H]);
create_a_regex_pattern([H | T], Acc) ->
	create_a_regex_pattern(T, [H, "|"] ++ Acc).

%%
create_list_of_regex_patterns([], Acc) -> Acc;
create_list_of_regex_patterns([{H, P} | T], Acc) ->
	create_list_of_regex_patterns(T, [{H, create_a_regex_pattern(P,[])}] ++ Acc).

extract_key_value({ResourceString, 'none', HashKeyName, _RangeKeyName}, {'hashkey', {_Max, _MaxLen, _Pattern, {_KeyName, Value}}}) ->
	[{ResourceString, [{HashKeyName, Value}]}];
extract_key_value({ResourceString, 'none', _HashKeyName, RangeKeyName}, {'rangekey', {_Max, _MaxLen, _Pattern, {_KeyName, Value}}}) ->
	[{ResourceString, [{RangeKeyName, Value}]}];
extract_key_value({ResourceString, Id, HashKeyName, RangeKeyName}, {'hashkey', {_Max, _MaxLen, _Pattern, {_KeyName, Value}}}) ->
	[{ResourceString, [{HashKeyName, Value}, {RangeKeyName, Id}]}];
extract_key_value({ResourceString, Id, HashKeyName, RangeKeyName}, {'rangekey', {_Max, _MaxLen, _Pattern, {_KeyName, Value}}}) ->
	[{ResourceString, [{HashKeyName, Id}, {RangeKeyName, Value}]}].

%% try to match both hashkey and rangekey
extract_key_value(  {ResourceString, 'none', HashKeyName, RangeKeyName},
                    {'hashkey' , {_Max1, _MaxLen1, _Pattern1, {_KeyName1, Value1}}},
                    {'rangekey', {_Max2, _MaxLen2, _Pattern2, {_KeyName2, Value2}}}) ->
	[{ResourceString, [{HashKeyName, Value1}, {RangeKeyName, Value2}]}];

extract_key_value(  {ResourceString, 'none', HashKeyName, RangeKeyName},
                    {'rangekey', {_Max1, _MaxLen1, _Pattern1, {_KeyName1, Value1}}},
                    {'hashkey', {_Max2, _MaxLen2, _Pattern2, {_KeyName2, Value2}}}) ->
	[{ResourceString, [{HashKeyName, Value2}, {RangeKeyName, Value1}]}];

extract_key_value(TableInfo, {Key1, {Max1, MaxLen1, Pattern1, KeyValue1}}, {Key2, {Max2, MaxLen2, Pattern2, KeyValue2}}) ->
	case ((Max1>Max2) or ((Max1==Max2) and (MaxLen1>MaxLen2))) of
		true -> extract_key_value(TableInfo, {Key1, {Max1, MaxLen1, Pattern1, KeyValue1}});
		_ ->	extract_key_value(TableInfo, {Key2, {Max2, MaxLen2, Pattern2, KeyValue2}})
	end.


create_list_of_regex_patterns_for_params([], _, Acc) -> Acc;
create_list_of_regex_patterns_for_params([{ParamName, _ParamValue} | TP], {KeyType, KeyName}, Acc) ->
	create_list_of_regex_patterns_for_params(TP, {KeyType, KeyName}, create_list_of_regex_patterns([{KeyType , [ParamName]}], []) ++ Acc).

match_hashkey_on_strings(Params, [], Result) ->
	{KeyType, {Count,Length, Pattern, _KeyValue}} = Result,
	case KeyType of
		'none' -> {'none', {0, 0, 'none', 'none'}};
		_ ->
			%% locate the correct value
			[{K,V}] = lists:filter(fun(X) -> {K,_V}=X, Pattern == lists:concat(["(", K, ")"]) end, Params),
			{KeyType, {Count,Length, Pattern, {K, V}}}
	end;

match_hashkey_on_strings(Params, [{KeyType, KeyName} | T], {MaxKeyType, {MaxCount, MaxLength, MaxPattern, MaxKeyValue}}) ->
	Pattern = create_list_of_regex_patterns_for_params(Params, {KeyType , KeyName}, []),
	CombinedParams = [{KeyName,'none'}],
	{CurrKeyType, {CurrCount, CurrLength, CurrPattern, CurrKeyValue}} = match_hashkey(Pattern, CombinedParams),
	case ((CurrCount > MaxCount) or ((CurrCount == MaxCount) and (CurrLength>MaxLength))) of
		true ->
			match_hashkey_on_strings(Params, T, {CurrKeyType, {CurrCount, CurrLength, CurrPattern, CurrKeyValue}});
		_ ->
			match_hashkey_on_strings(Params, T, {MaxKeyType, {MaxCount, MaxLength, MaxPattern, MaxKeyValue}})
	end.



locate_the_most_prabable_key_values(Parameters, Acc, TableKeyPatterns, KeyPatterns) ->
	% Create a list of {Name, Value}, combined of URL params and Path
	CombinedParams = concat_parameters_key_value(Parameters, concat_parameters_table_keys(Acc, [])),
	% match CombinedParams against Patterns, pick first found
	{KeyType, PossibleKey} = match_hashkey(TableKeyPatterns, CombinedParams),

	CombinedParams2 = concat_parameters_key_value(Parameters, concat_parameters_incl_table_keys(Acc, [])),
	{KeyType2, PossibleKey2} = match_hashkey_on_strings(CombinedParams2, KeyPatterns, {'none',{0, 0, [], []}}),

	{{KeyType, PossibleKey}, {KeyType2, PossibleKey2}}.

resources_as_tables([], Acc) -> Acc;
resources_as_tables([Resource | T], Acc) ->
	case is_resource_a_table(Resource) of
		true -> resources_as_tables(T, [Resource]++Acc);
		_ ->resources_as_tables(T, Acc)
	end.

extract_table(Resource, []) -> resources_as_tables([Resource], []);
extract_table(Resource, [{Table, _Keys} | _H]) ->
	ResourceString = binary:bin_to_list(Resource),
	resources_as_tables([
						Resource,
						binary:list_to_bin(lists:append([ResourceString,"_",Table])),
						binary:list_to_bin(lists:append([Table,"_",ResourceString]))
						], []).


extract_resource_keys([], _Parameters, Acc) -> Acc;
extract_resource_keys([Resource | T], Parameters, Acc) ->
	ResourceString = binary:bin_to_list(Resource),
	Keys = extract_names(table_keys(Resource), []),
	case length(Keys) of
	1 ->
		[HashKeyName] = Keys,
		Patterns = create_list_of_regex_patterns([{'hashkey' , [ResourceString]}
												, {'hashkey' , [HashKeyName]}
												, {'hashkey' , [ResourceString, HashKeyName]}], []),

		{{KeyType, PossibleKey}, {KeyType2, PossibleKey2}} = locate_the_most_prabable_key_values(
																Parameters, Acc, Patterns,
																[{'hashkey' , HashKeyName}, {'hashkey' , ResourceString}]),

		case ((KeyType=='none') and (KeyType2=='none')) of
		true ->
				extract_resource_keys(T, Parameters, [{ResourceString, []}] ++ Acc);
		_ ->
				extract_resource_keys(T, Parameters, extract_key_value( {ResourceString, 'none', HashKeyName, 'none'}, {KeyType, PossibleKey}, {KeyType2, PossibleKey2}) ++ Acc)
		end;
	2 ->
		[HashKeyName, RangeKeyName] = Keys,
		%% create patterns to compare in the switched order of significancy (regex reorder), first found then stop
		Patterns = create_list_of_regex_patterns([{'hashkey' , [ResourceString]}
												, {'rangekey', [RangeKeyName]}
												, {'rangekey', [ResourceString, RangeKeyName]}
												, {'hashkey' , [HashKeyName]}
												, {'hashkey' , [ResourceString, HashKeyName]}], []),

		{{KeyType, PossibleKey}, {KeyType2, PossibleKey2}} = locate_the_most_prabable_key_values(
																Parameters, Acc, Patterns,
																[{'hashkey' , HashKeyName}, {'rangekey', RangeKeyName}, {'hashkey' , ResourceString}]),

		case ((KeyType=='none') and (KeyType2=='none')) of
		true ->
				extract_resource_keys(T, Parameters, [{ResourceString, []}] ++ Acc);
		_ ->
				extract_resource_keys(T, Parameters, extract_key_value( {ResourceString, 'none', HashKeyName, RangeKeyName}, {KeyType, PossibleKey}, {KeyType2, PossibleKey2}) ++ Acc)
		end
	end.


extract_resource_keys([], [], _Parameters, Acc) -> Acc;
extract_resource_keys([], [_Id |Next], Parameters, Acc) ->
	match_resource_to_keys(Next, Parameters, Acc);
extract_resource_keys([Resource | T], [Id |Next], Parameters, Acc) ->
	ResourceString = binary:bin_to_list(Resource),
	Keys = extract_names(table_keys(Resource), []),
	case length(Keys) of
		1 ->
			[HashKeyName] = Keys,
			extract_resource_keys(T, [Id |Next], Parameters, [{binary:bin_to_list(Resource), [{HashKeyName, binary:bin_to_list(Id)}]}] ++ Acc);
		2 ->
			[HashKeyName, RangeKeyName] = Keys,
			%% create patterns to compare in the switched order of significancy (regex reorder), first found then stop
			Patterns = create_list_of_regex_patterns([{'hashkey' , [ResourceString]}
													, {'rangekey', [RangeKeyName]}
													, {'rangekey', [ResourceString, RangeKeyName]}
													, {'hashkey' , [HashKeyName]}
													, {'hashkey' , [ResourceString, HashKeyName]}], []),

			{{KeyType, PossibleKey}, {KeyType2, PossibleKey2}} = locate_the_most_prabable_key_values(
																	Parameters, Acc, Patterns,
																	[{'hashkey' , HashKeyName}, {'rangekey', RangeKeyName}, {'hashkey' , ResourceString}]),

			case ((KeyType=='none') and (KeyType2=='none')) of
			true -> %% value could not be matched, assume the id is a hashkey
					extract_resource_keys(T, [Id |Next], Parameters, [{ResourceString, [{HashKeyName, binary:bin_to_list(Id)}]}] ++ Acc);
			_ ->
					extract_resource_keys(T, [Id |Next], Parameters, extract_key_value( {ResourceString, binary:bin_to_list(Id), HashKeyName, RangeKeyName}, {KeyType, PossibleKey}, {KeyType2, PossibleKey2}) ++ Acc)
			end
	end.

extract_items([]) -> [];
extract_items([{<<"Items">>,R} | _T]) -> R;
extract_items([{<<"Item">>,R} | _T]) -> [R];
extract_items([_H | T]) -> extract_items(T).

convert_to_bin([], Acc) -> Acc;
convert_to_bin([{K, V} | T], Acc) ->
	convert_to_bin(T, [{binary:list_to_bin(K), binary:list_to_bin(V)}] ++ Acc).

% get_item
get_item('scan', TableName, _Keys, KeyValues, Parameters) ->
	Params = KeyValues ++ Parameters,
	P = extract_fields('scan', Params,[]),
	case ddb:scan(TableName, P) of
		{ok,Results} -> {ok, extract_items(Results)};
		_ -> {error, []}
	end;
get_item('get', TableName, [{_KeyName,KeyType}], [{_KeyName,KeyValue}], Parameters) ->
	P = extract_fields('get', Parameters,[]),
	case ddb:get(TableName, ddb:key_value(KeyValue, KeyType), P) of
		{ok, Results} -> {ok, extract_items(Results)};
		_ -> {error, []}
	end;
get_item('get', TableName, [{_RangeKeyName,RangeKeyType}, {_HashKeyName,HashKeyType}],
				[{_RangeKeyName,RangeKeyValue}, {_HashKeyName,HashKeyValue}], _Parameters) ->
	case ddb:find(TableName, {HashKeyValue, HashKeyType}, {'equal', RangeKeyType, [RangeKeyValue]}) of
		{ok,Results} -> {ok, extract_items(Results)};
		_ -> {error, []}
	end.

% delete_item
delete_item(TableName, [{_KeyName,KeyType}], [{_KeyName,KeyValue}], [{ParamName, ParamValue}]) ->
	case ddb:cond_delete(TableName, ddb:key_value(KeyValue, KeyType), {'exists', ParamName, ParamValue, 'string'}, 'none') of
		{ok, _} -> {ok, []};
		_ -> {error, []}
	end;
delete_item(TableName, [{_KeyName,KeyType}], [{_KeyName,KeyValue}], []) ->
	case ddb:delete(TableName, ddb:key_value(KeyValue, KeyType), 'none') of
		{ok, _} -> {ok, []};
		_ -> {error, []}
	end;
delete_item(TableName, [{_RangeKeyName,RangeKeyType}, {_HashKeyName,HashKeyType}],
	[{_RangeKeyName,RangeKeyValue}, {_HashKeyName,HashKeyValue}], [{ParamName, ParamValue}])  ->
	case ddb:cond_delete(TableName, ddb:key_value(HashKeyValue, HashKeyType, RangeKeyType, RangeKeyValue), {'exists', ParamName, ParamValue, 'string'}, 'none') of
		{ok, _} -> {ok, []};
		_ -> {error, []}
	end;
delete_item(TableName, [{_RangeKeyName,RangeKeyType}, {_HashKeyName,HashKeyType}],
	[{_RangeKeyName,RangeKeyValue}, {_HashKeyName,HashKeyValue}], [])  ->
	case ddb:delete(TableName, ddb:key_value(HashKeyValue, HashKeyType, RangeKeyType, RangeKeyValue), 'none') of
		{ok, _} -> {ok, []};
		_ -> {error, []}
	end.

% put_item
put_item(TableName, [{KeyName,KeyType}] , [{_KeyName,KeyValue}], Attributes) ->
	case ddb:update(TableName, ddb:key_value(KeyValue, KeyType), Attributes, 'all_new') of
		{ok, Results} -> {ok, extract_items(Results)};
		_ -> {error, []}
	end;
put_item(TableName, [{RangeKeyName,RangeKeyType}, {HashKeyName,HashKeyType}],
			[{_RangeKeyName,RangeKeyValue}, {_HashKeyName,HashKeyValue}], Attributes) ->
	case ddb:update(TableName, ddb:key_value(HashKeyValue, HashKeyType, RangeKeyType, RangeKeyValue), Attributes, 'all_new') of
		{ok, Results} -> {ok, extract_items(Results)};
		_ -> {error, []}
	end.

% post_item
post_item(TableName, Attributes) ->
	case ddb:put(TableName, Attributes) of
		{ok, Results} -> {ok, extract_items(Results)};
		_ -> {error, []}
	end.

%% extract fields
extract_fields(_, [], []) -> [];
extract_fields('scan',[], ScanAcc) -> [{<<"ScanFilter">>,ScanAcc}];
extract_fields('get',[], _ScanAcc) -> [];
extract_fields(Type,[H | T], ScanAcc) ->
	case H of
		{<<"fields">>,Fields} ->
			[{<<"AttributesToGet">>,strings_to_bins(string:tokens(binary:bin_to_list(Fields) ,","))}] ++ extract_fields(Type,T, ScanAcc);
		{<<"limit">>,LimitValue} ->
			[{<<"Limit">>, list_to_integer(binary:bin_to_list(LimitValue))}] ++ extract_fields(Type,T, ScanAcc);
		{<<"offset">>,StartKey} ->
			[{<<"ExclusiveStartKey">>, extract_offset_key(binary:bin_to_list(StartKey))}] ++ extract_fields(Type,T, ScanAcc);
		{Name,Value} ->
			{Operator, AdjValue} = extract_operator(Value),
			extract_fields(Type, T, [{Name, [{<<"AttributeValueList">>, [[{<<"S">>, AdjValue}]]},{<<"ComparisonOperator">>, Operator}]}] ++ ScanAcc)
	end.

%% is_resource_a_table(fun application:get_env/2, <<"job">>
is_resource_a_table(Resource) ->
	%% Tables is list of binarystrings
	{ok, Tables} = application:get_env('rest2ddb', 'aws_tables'),
	lists:member(Resource, Tables).

% table_details(fun application:get_env/2, <<"job">>
table_details(TableName) ->
	{ok, TablesDetails} = application:get_env('rest2ddb', 'aws_tables_details'),
 	lists:filter(fun(X) -> case X of {table,TableName,_,_}  -> true; _ -> false end end, TablesDetails).

% table_keys
table_keys(TableName) ->
	Details=table_details(TableName),
	extract_keys(
	  tuple_to_list(lists:append(Details)),
	  []).

%% extract_operator
extract_operator(Value) ->
	case binary:first(Value) of
	  42 -> %% * symbol
		{<<"CONTAINS">>, binary:part(Value,1,byte_size(Value)-1)};
	_ ->
		{<<"EQ">>, Value}
	end.

%% strings_to_bins
strings_to_bins(L) ->
	lists:map(fun(X) -> binary:list_to_bin(X) end, L).

%% extract_offset_key
extract_offset_key(Value) ->
	extract_offset_key(string:tokens(Value, ",)("), []).

extract_offset_key([], Acc) -> Acc;
extract_offset_key([H | T], Acc) ->
	{Key,Type,Value}=list_to_tuple(string:tokens(H, "=:")),
	extract_offset_key(T, [{Key, {Type , Value}}] ++ Acc).


%% extract_keys
extract_keys([], Acc) -> Acc;
extract_keys([{aws_base_attribute,undefined,undefined} | H], Acc) ->
	extract_keys(H, Acc);
extract_keys([{aws_base_attribute,AttName,AttType} | H], Acc) ->
	extract_keys(H, [{AttName,AttType} | Acc]);
extract_keys([_A | H], Acc) ->
	extract_keys(H, Acc).

%% extract_names
extract_names([], Acc) -> Acc;
extract_names([{KeyName,_} | H], Acc) ->
	Keys = string:tokens(binary:bin_to_list(KeyName),":"),
	extract_names(H, Keys ++ Acc);
extract_names([_ | H], Acc) ->
	extract_names(H, Acc).

extract_type(<<"S">>) -> 'string';
extract_type(<<"N">>) -> 'number';
extract_type(<<"SS">>) -> ['string'];
extract_type(<<"NS">>) -> ['number'].

extract_from_json([Name, Type, Value | T], 'none', AccParams, AccBody) ->
	extract_from_json(T,
		'none',
		[{binary:list_to_bin(Name), binary:list_to_bin(Value)}] ++ AccParams,
		[{binary:list_to_bin(Name), binary:list_to_bin(Value), extract_type(binary:list_to_bin(Type)) }] ++ AccBody
		);
extract_from_json([Name, Type, Value | T], Action, AccParams, AccBody) ->
	extract_from_json(T,
		Action,
		[{binary:list_to_bin(Name), binary:list_to_bin(Value)}] ++ AccParams,
		[{binary:list_to_bin(Name), binary:list_to_bin(Value), extract_type(binary:list_to_bin(Type)), Action}] ++ AccBody
		);
extract_from_json(_, _, AccParams, AccBody) -> {AccParams, AccBody}.

%% unit tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

pattern_match_keys_test() ->
    ?assertEqual({2,6,"(user|id)",{"user_id","1"}}, pattern_match_keys("(user|id)", [{"user_id", "1"}])),
    ?assertEqual({1,2,"(job|id)",{"user_id","1"}}, pattern_match_keys("(job|id)",  [{"user_id", "1"}]) ),
    ?assertEqual({2,5,"(job|id)",{"job_id","5"}}, pattern_match_keys("(job|id)",  [{"user_id", "1"}, {"job_id", "5"}]) ).

match_hashkey_test() ->
    ?assertEqual({'none',{0,0,[],[]}}
        ,match_hashkey([], [])),
    ?assertEqual({'none',{0,0,[],[]}}
        ,match_hashkey([{'hashkey',"(id|user)"}], [])),
    ?assertEqual({'hashkey',{2,6,"(id|user)",{"user_id","1"}}}
        ,match_hashkey([{'hashkey',"(id|user)"}], [{"user_id", "1"}])),
    ?assertEqual({hashkey,{2,6,"(id|user)",{"user_id","1"}}}
        ,match_hashkey([{'hashkey',"(id|user)"}], [{"fields","name,id"}, {"user_id", "1"}])),
    ?assertEqual({hashkey,{2,6,"(id|user)",{"user_id","1"}}}
        ,match_hashkey([{'hashkey',"(id|user)"}], [{"fields","name,id"}, {"user_job","5"}, {"user_id", "1"}])),
    ?assertEqual({hashkey,{2,6,"(id|user)",{"user_id","1"}}}
        ,match_hashkey([{'hashkey',"(id|user)"}], [{"fields","name,id"}, {"user_job","5"}, {"user_id", "1"}, {"job_id","2"}])),
    ?assertEqual({hashkey,{2,6,"(id|user)",{"user_id","1"}}}
        ,match_hashkey([{'hashkey',"(email|task)"}, {'hashkey',"(id|user)"}], [{"fields","name,id"}, {"user_job","5"}, {"user_id", "1"}, {"job_id","2"}])),
    ?assertEqual({hashkey,{1,4,"(user)",{"user_job","5"}}}
        ,match_hashkey([{'hashkey',"(user)"}, {'hashkey',"(id|user)"}], [{"fields","name,id"}, {"user_job","5"}, {"user_id", "1"}, {"job_id","2"}])),
    ?assertEqual({hashkey,{1,4,"(user)",{"user_id","1"}}}
        ,match_hashkey([{'hashkey',"(user)"}, {'hashkey',"(id|user)"}], [{"fields","name,id"}, {"user_id", "1"}, {"user_job","5"}, {"job_id","2"}])).

create_a_regex_pattern_test() ->
    ?assertEqual("()", create_a_regex_pattern([],[])),
    ?assertEqual("(user)", create_a_regex_pattern(["user"],[])),
    ?assertEqual("(id|user)", create_a_regex_pattern(["user", "id"],[])),
    ?assertEqual("(email|id|user)", create_a_regex_pattern(["user", "id", "email"],[])).

create_list_of_regex_patterns_test() ->
    ?assertEqual([], create_list_of_regex_patterns([],[])),
    ?assertEqual([{'hash',"(user)"}], create_list_of_regex_patterns([{'hash',["user"]}],[])),
    ?assertEqual([{'range',"(id)"},{'hash',"(email|user)"}], create_list_of_regex_patterns([{'hash',["user","email"]},{'range',["id"]}],[])),
    ?assertEqual([{hashkey,"(id|user)"}], create_list_of_regex_patterns([{'hashkey' , ["user", "id"]}], []) ).

extract_operator_test() ->
    ?assertEqual({<<"CONTAINS">>,<<"a">>}, extract_operator(<<"*a">>)),
    ?assertEqual({<<"EQ">>,<<"xyz">>}, extract_operator(<<"xyz">>)).

strings_to_bins_test() ->
    ?assertEqual([<<"name">>,<<"id">>], strings_to_bins(["name","id"])).

extract_from_json_test() ->
		Body = <<"[{\"role\":{\"S\":\"user\"},\"email\":{\"S\":\"test@test.com\"},\"lastName\":{\"S\":\"lasttest\"},\"firstName\":{\"S\":\"firsttest\"},\"user_id\":{\"N\":\"1\"},\"atts\":{\"S\":\"some extra stuff\"}}]">>,
		{Attributes, ConvertedBody} = extract_from_json(string:tokens(binary:bin_to_list(Body),":,{}][\""), 'none', [], []),
		?assertEqual([{<<"atts">>, <<"some extra stuff">>},
									{<<"user_id">>, <<"1">>},
									{<<"firstName">>, <<"firsttest">>},
									{<<"lastName">>, <<"lasttest">>},
									{<<"email">>, <<"test@test.com">>},
									{<<"role">>, <<"user">>}]
									, Attributes),
		?assertEqual([{<<"atts">>, <<"some extra stuff">>, 'string'},
									{<<"user_id">>, <<"1">>, 'number'},
									{<<"firstName">>, <<"firsttest">>, 'string'},
									{<<"lastName">>, <<"lasttest">>, 'string'},
									{<<"email">>, <<"test@test.com">>, 'string'},
									{<<"role">>, <<"user">>, 'string'}]
									, ConvertedBody).

extract_offset_key_test() ->
    ?assertEqual( [{"RangeKeyElement",{"N","250"}},{"HashKeyElement",{"S","Riley"}}], extract_offset_key("(HashKeyElement=S:Riley,RangeKeyElement=N:250)")).

extract_keys_hashkey_test() ->
    Details = [{table,<<"user">>,
        {aws_base_attribute,<<"user_id">>,number},
        {aws_base_attribute,undefined,undefined}}
    ],
    ?assertEqual([{<<"user_id">>,number}], extract_keys( tuple_to_list(lists:append(Details)), []) ).

extract_keys_hashkey_rangekey_test() ->
    Details = [{table,<<"user_job">>,
        {aws_base_attribute,<<"user_id">>,number},
        {aws_base_attribute,<<"job_id">>,number}}
    ],
    ?assertEqual([{<<"job_id">>,number},{<<"user_id">>,number}], extract_keys( tuple_to_list(lists:append(Details)), [])).

extract_fields_test() ->
    ?assertEqual([{<<"AttributesToGet">>,[<<"name">>, <<"id">>]}]
                ,extract_fields('scan',[{<<"fields">>,<<"name,id">>}],[])),

    ?assertEqual([{<<"ScanFilter">>,[{<<"name">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"John">>}]]},{<<"ComparisonOperator">>, <<"EQ">>}]}]}]
                ,extract_fields('scan',[{<<"name">>,<<"John">>}],[])),

    ?assertEqual([{<<"ScanFilter">>,[
                            {<<"name">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"Jo">>}]]},{<<"ComparisonOperator">>, <<"CONTAINS">>}]}
                            ]
                    }]
                ,extract_fields('scan',[{<<"name">>,<<"*Jo">>}],[])),

    ?assertEqual([{<<"ScanFilter">>,[
                            { <<"surname">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"K">>}]]},{<<"ComparisonOperator">>, <<"CONTAINS">>}] },
                            { <<"name">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"Jo">>}]]},{<<"ComparisonOperator">>, <<"EQ">>}] }
                            ]
                    }]
                ,extract_fields('scan',[{<<"name">>,<<"Jo">>}, {<<"surname">>,<<"*K">>}],[])),

    ?assertEqual([{<<"AttributesToGet">>,[<<"name">>, <<"id">>]},
                    {<<"ScanFilter">>,[
                            {<<"name">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"Jo">>}]]},{<<"ComparisonOperator">>, <<"CONTAINS">>}]}
                            ]
                    }]
                ,extract_fields('scan',[{<<"fields">>,<<"name,id">>}, {<<"name">>,<<"*Jo">>}],[])),

    ?assertEqual([{<<"AttributesToGet">>,[<<"name">>, <<"id">>]},
                    {<<"ScanFilter">>,[
                            {<<"surname">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"K">>}]]},{<<"ComparisonOperator">>, <<"CONTAINS">>}]},
                            {<<"name">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"Jo">>}]]},{<<"ComparisonOperator">>, <<"EQ">>}]}
                            ]
                    }]
                ,extract_fields('scan',[{<<"name">>,<<"Jo">>}, {<<"fields">>,<<"name,id">>}, {<<"surname">>,<<"*K">>}],[])),

    ?assertEqual([{<<"AttributesToGet">>,[<<"name">>, <<"id">>]},
                    {<<"Limit">>,2},
                    {<<"ScanFilter">>,[
                            {<<"name">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"Jo">>}]]},{<<"ComparisonOperator">>, <<"EQ">>}]}
                            ]
                    }]
                ,extract_fields('scan',[{<<"name">>,<<"Jo">>}, {<<"fields">>,<<"name,id">>}, {<<"limit">>,<<"2">>}],[])),

    ?assertEqual([{<<"ExclusiveStartKey">>,
                          [{"RangeKeyElement",{"N","250"}},
                           {"HashKeyElement",{"S","Riley"}}]
                    }
                 ]
                ,extract_fields('scan',[{<<"offset">>, <<"(HashKeyElement=S:Riley,RangeKeyElement=N:250)">>}],[])),

    ?assertEqual([{<<"Limit">>,2},
                    {<<"ExclusiveStartKey">>,
                          [{"RangeKeyElement",{"N","250"}},
                           {"HashKeyElement",{"S","Riley"}}]
                    }
                    ]
                ,extract_fields('scan',[{<<"limit">>,<<"2">>}
                    ,{<<"offset">>, <<"(HashKeyElement=S:Riley,RangeKeyElement=N:250)">>}],[])),

    ?assertEqual([{<<"AttributesToGet">>,[<<"name">>, <<"id">>]},
                    {<<"Limit">>,2},
                    {<<"ExclusiveStartKey">>,
                          [{"RangeKeyElement",{"N","250"}},
                           {"HashKeyElement",{"S","Riley"}}]
                    }
                 ]
                ,extract_fields('scan',[{<<"fields">>,<<"name,id">>}, {<<"limit">>,<<"2">>}
                    ,{<<"offset">>, <<"(HashKeyElement=S:Riley,RangeKeyElement=N:250)">>}],[])),

    ?assertEqual([{<<"AttributesToGet">>,[<<"name">>, <<"id">>]},
                    {<<"Limit">>,2},
                    {<<"ExclusiveStartKey">>,
                          [{"RangeKeyElement",{"N","250"}},
                           {"HashKeyElement",{"S","Riley"}}]
                    },
                    {<<"ScanFilter">>,[
                            {<<"name">>, [{<<"AttributeValueList">>, [[{<<"S">>, <<"Jo">>}]]},{<<"ComparisonOperator">>, <<"EQ">>}]}
                            ]
                    }]
                ,extract_fields('scan',[{<<"name">>,<<"Jo">>}, {<<"fields">>,<<"name,id">>}, {<<"limit">>,<<"2">>},
                      {<<"offset">>, <<"(HashKeyElement=S:Riley,RangeKeyElement=N:250)">>}],[])).

extract_items_test() ->
    ?assertEqual(	[[{<<"role">>,[{<<"S">>,<<"administrator">>}]},
                      {<<"email">>,[{<<"S">>,<<"admin@test.com">>}]},
                      {<<"lastName">>,[{<<"S">>,<<"admin">>}]},
                      {<<"firstName">>,[{<<"S">>,<<"admin">>}]},
                      {<<"user_id">>,[{<<"N">>,<<"2">>}]},
                      {<<"atts">>,[{<<"S">>,<<"some extra stuff2">>}]}
                     ]],
        extract_items(
                        [	{<<"ConsumedCapacityUnits">>,0.5},
                  {<<"Count">>,1},
                  {<<"Items">>,
                   [[{<<"role">>,[{<<"S">>,<<"administrator">>}]},
                     {<<"email">>,[{<<"S">>,<<"admin@test.com">>}]},
                     {<<"lastName">>,[{<<"S">>,<<"admin">>}]},
                     {<<"firstName">>,[{<<"S">>,<<"admin">>}]},
                     {<<"user_id">>,[{<<"N">>,<<"2">>}]},
                     {<<"atts">>,[{<<"S">>,<<"some extra stuff2">>}]}]]},
                  {<<"LastEvaluatedKey">>,
                   [{<<"HashKeyElement">>,[{<<"N">>,<<"2">>}]}]},
                  {<<"ScannedCount">>,1}
                 ])).



environment_mockup_test() ->
    application:set_env('rest2ddb', 'aws_tables', rest2ddb_utils_mockups:environment_mockup('aws_tables')),
    application:set_env('rest2ddb', 'aws_tables_details', rest2ddb_utils_mockups:environment_mockup('aws_tables_details')),
    ?assertEqual(true, is_resource_a_table(<<"job">>)),
    ?assertEqual(false, is_resource_a_table(<<"tableWhichDoesNotExist">>)),
    ?assertEqual([{<<"job">>,number},{<<"user">>,number}], table_keys(<<"user_job">>)),
    ?assertEqual(["user","job"], extract_names(table_keys(<<"user_job">>), [])),
    ?assertEqual([{table,<<"job">>,
                    {aws_base_attribute,<<"id">>,number},
                    {aws_base_attribute,undefined,undefined}
                 }], table_details(<<"job">>)).

-endif.
