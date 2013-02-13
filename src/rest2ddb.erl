%% @author milo hyben
%% @doc REST to DynamoDB interface.

-module(rest2ddb).

%% ====================================================================
%% API functions
%% ====================================================================

-export([
		init/4,
		get/2,
        extract_query/1,
        extract_path/1
        %,
		%put/3,
		%post/2
        %delete
		]).

-spec init(string(), string(), pos_integer(), string()) -> 'ok'.
%% @doc read tables schema information from DynamoDB and store it in application environment settings
init(AccessKeyId, SecretAccessKey, DurationInSeconds, _Region) ->
    ddb_iam:credentials(AccessKeyId, SecretAccessKey),
    {'ok', Key, Secret, Token} = ddb_iam:token(DurationInSeconds),
    ddb:credentials(Key, Secret, Token),
	{'ok', Tables} = ddb:tables(),
	'ok' = application:set_env('rest2ddb', 'aws_tables', Tables),
	'ok' = application:set_env('rest2ddb', 'aws_tables_details', parse_tables(Tables, [])).
    
-spec get([binary()], [binary()]) -> {ok, [[{binary(), [{binary(), binary()}]}]]} | {error, []}.
%% @doc retrieve records from DynamoDB matching {UrlPath, UrlParameters}   
%% where UrlPath is list
get(UrlPath, UrlParameters) ->
	rest2ddb_utils:find_resource(UrlPath, UrlParameters).
    
-spec extract_query(binary()) -> [binary()].    
%% @doc Convert Query String part of URL into a list
extract_query(Path) ->    
    case binary:split(Path, [<<"?">>]) of
        [_, Qs] -> token_binary(Qs, [<<"&">>], []);
        [_]     -> <<>>
    end.

-spec extract_path(binary()) -> [binary()].
%% @doc Convert Path part of URL into a list
extract_path(Path) ->    
    case binary:split(Path, [<<"?">>]) of
        [Raw, _] -> token_binary(Raw, [<<"/">>], []);
        [_]     -> <<>>
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc aws_base_type
-type aws_base_type() :: 'string' | 'number'.

%% @doc aws_base_attribute
-record(aws_base_attribute, {
	name :: string(),
	type :: aws_base_type()
}).

%% @doc table
-record(table, {
	name :: string(),
	hash_key_element = #aws_base_attribute{}, %% aws_attribute
	range_key_element = #aws_base_attribute{} %% aws_attribute
}).

%% @doc token_binary
token_binary(B, Token, Acc) ->
    case binary:split(B, Token) of
        [A, T] -> token_binary(T, Token, Acc++[A]);
        [Rest] -> Acc++[Rest]
    end.

%% @doc parse aws base attribute
parse_aws_attribute([{<<"AttributeName">>,Name},{<<"AttributeType">>,<<"S">>}]) ->
	#aws_base_attribute{name = Name, type='string'};
parse_aws_attribute([{<<"AttributeName">>,Name},{<<"AttributeType">>,<<"N">>}]) ->
	#aws_base_attribute{name = Name, type='number'}.
%%parse_aws_attribute([{<<"AttributeName">>,Name},{<<"AttributeType">>,<<"B">>}]) ->
%%	#aws_base_attribute{name = Name, type='B'}.


%% @doc parse table only with HashKey
parse_table({ok,[{<<"Table">>,[ _,_,{<<"KeySchema">>
							,[{<<"HashKeyElement">>,HashKey}]}
							,_,{<<"TableName">>,TableName},_,_]}]}) ->
	#table{name = TableName,
		   hash_key_element = parse_aws_attribute(HashKey)
		  };

%% @doc parse table with HashKey & RangeKey
parse_table({ok,[{<<"Table">>,[ _,_,{<<"KeySchema">>
							,[{<<"HashKeyElement">>,HashKey}
							,{<<"RangeKeyElement">>,RangeKey}]}
							,_,{<<"TableName">>,TableName},_,_]}]}) ->
	#table{name = TableName,
		   hash_key_element = parse_aws_attribute(HashKey),
		   range_key_element=parse_aws_attribute(RangeKey)
		  }.


%% @doc parse tables
parse_tables([], Acc) -> Acc;
parse_tables([H | T], Acc) ->
	parse_tables(T,
		[ parse_table( ddb:describe_table(H))]
		++ Acc).

