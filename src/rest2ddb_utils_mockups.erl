%% @author milo
%% @doc @todo Add description to bto_resource_handler.

-module(rest2ddb_utils_mockups).

% unit tests mockup functions
-ifdef(TEST).

-export([
		environment_mockup/1,
		ddb_get_mockup/3,
		ddb_scan_mockup/2,
		ddb_find_mockup/3,
		ddb_key_value_mockup/2,
		ddb_describe_table/1,
		ddb_tables/0,
		table_key_info/1
		]).


environment_mockup('aws_tables') ->
	[<<"job">>,<<"user">>,<<"task">>,<<"user_job">>];

environment_mockup('aws_tables_details') ->
	[{table,<<"user_job">>,
	            {aws_base_attribute,<<"user">>,number},
	            {aws_base_attribute,<<"job">>,number}},
	     {table,<<"user">>,
	            {aws_base_attribute,<<"id">>,number},
	            {aws_base_attribute,undefined,undefined}},
       {table,<<"task">>,
	            {aws_base_attribute,<<"user_id">>,number},
	            {aws_base_attribute,<<"task_id">>,string}},
	     {table,<<"job">>,
	            {aws_base_attribute,<<"id">>,number},
	            {aws_base_attribute,undefined,undefined}}].

ddb_get_mockup(	<<"user">>,
				[{<<"Key">>, [{<<"HashKeyElement">>, [{<<"N">>, <<"1">>}]}]}],
				[{<<"AttributesToGet">>,[<<"name">>,<<"user_id">>]}]
			) ->
{ok,[{<<"ConsumedCapacityUnits">>,0.5},
     {<<"Item">>,
      [
       {<<"name">>,[{<<"S">>,<<"John">>}]},
       {<<"user_id">>,[{<<"N">>,<<"1">>}]}
       ]
		}]
};

ddb_get_mockup(<<"user">>,[{<<"Key">>, [{<<"HashKeyElement">>, [{<<"N">>, <<"1">>}]}]}],[]) ->
{ok,[{<<"ConsumedCapacityUnits">>,0.5},
     {<<"Item">>,
      [
       {<<"name">>,[{<<"S">>,<<"John">>}]},
       {<<"user_id">>,[{<<"N">>,<<"1">>}]},
	     {<<"role">>,[{<<"S">>,<<"user">>}]},
       {<<"email">>,[{<<"S">>,<<"test@test.com">>}]},
       {<<"surname">>,[{<<"S">>,<<"Citizen">>}]}
       ]
		}]
}.

ddb_scan_mockup(<<"user">>,
				[
					{<<"AttributesToGet">>,[<<"role">>,<<"user_id">>]},
					{<<"ScanFilter">>, [{<<"role">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"user">>}]]}, {<<"ComparisonOperator">>,<<"EQ">>}]}]
					}
				]) ->
{ok,[
		{<<"ConsumedCapacityUnits">>,0.5},
		{<<"Count">>,1},
		{<<"Items">>,
			[[{<<"role">>,[{<<"S">>,<<"user">>}]},
			{<<"user_id">>,[{<<"N">>,<<"1">>}]}]]},
		{<<"ScannedCount">>,2}
	]
};
ddb_scan_mockup(<<"user">>,
				[
					{<<"AttributesToGet">>,[<<"role">>,<<"user_id">>]}
				]) ->
{ok,[
		{<<"ConsumedCapacityUnits">>,0.5},
		{<<"Count">>,2},
		{<<"Items">>,[
			[
				{<<"role">>,[{<<"S">>,<<"administrator">>}]},
				{<<"user_id">>,[{<<"N">>,<<"2">>}]}
			],
            [
				{<<"role">>,[{<<"S">>,<<"user">>}]},
				{<<"user_id">>,[{<<"N">>,<<"1">>}]}
			]
		]},
		{<<"ScannedCount">>,2}
	]
};
ddb_scan_mockup(<<"user">>,
				[
					{<<"AttributesToGet">>,[<<"name">>,<<"user_id">>]},
					{<<"ScanFilter">>, [{<<"name">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"John">>}]]}, {<<"ComparisonOperator">>,<<"EQ">>}]}]
					}
				]) ->
{ok,[
		{<<"ConsumedCapacityUnits">>,0.5},
		{<<"Count">>,1},
		{<<"Items">>, [
			[
			   {<<"name">>,[{<<"S">>,<<"John">>}]},
			   {<<"user_id">>,[{<<"N">>,<<"1">>}]}
			]
		]},
		{<<"ScannedCount">>,2}
	]
};

ddb_scan_mockup(<<"user">>,
				[
					{<<"AttributesToGet">>,[<<"name">>,<<"user_id">>,<<"surname">>]},
					{<<"ScanFilter">>, [
						{<<"surname">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"C">>}]]}, {<<"ComparisonOperator">>,<<"CONTAINS">>}]},
						{<<"name">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"John">>}]]}, {<<"ComparisonOperator">>,<<"EQ">>}]}
					]
					}
				]) ->
{ok,[
		{<<"ConsumedCapacityUnits">>,0.5},
		{<<"Count">>,1},
		{<<"Items">>, [
			[
			   {<<"name">>,[{<<"S">>,<<"John">>}]},
			   {<<"user_id">>,[{<<"N">>,<<"1">>}]},
			   {<<"surname">>,[{<<"S">>,<<"Citizen1">>}]}
			]
		]},
		{<<"ScannedCount">>,2}
	]
};

ddb_scan_mockup(<<"task">>,
				[
					{<<"AttributesToGet">>,[<<"user_id">>,<<"task_id">>,<<"description">>]},
					{<<"ScanFilter">>, [
						{<<"description">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"T">>}]]}, {<<"ComparisonOperator">>,<<"CONTAINS">>}]},
						{<<"user_id">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"1">>}]]}, {<<"ComparisonOperator">>,<<"EQ">>}]}
					]
					}
				]) ->
{ok,[
		{<<"ConsumedCapacityUnits">>,0.5},
		{<<"Count">>,1},
		{<<"Items">>, [
			[
			   {<<"user_id">>,[{<<"N">>,<<"1">>}]},
			   {<<"task_id">>,[{<<"S">>,<<"2">>}]},
			   {<<"description">>,[{<<"S">>,<<"Task TODO">>}]}
			]
		]},
		{<<"ScannedCount">>,2}
	]
};


ddb_scan_mockup(<<"task">>,
				[
					{<<"AttributesToGet">>,[<<"user_id">>,<<"task_id">>,<<"description">>]},
					{<<"ScanFilter">>, [
						{<<"task_id">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"2">>}]]}, {<<"ComparisonOperator">>,<<"EQ">>}]},
						{<<"user_id">>, [{<<"AttributeValueList">>,[[{<<"S">>,<<"1">>}]]}, {<<"ComparisonOperator">>,<<"EQ">>}]}
					]
					}
				]) ->
{ok,[
		{<<"ConsumedCapacityUnits">>,0.5},
		{<<"Count">>,1},
		{<<"Items">>, [
			 [  {<<"user_id">>,[{<<"N">>,<<"1">>}]},
			   {<<"task_id">>,[{<<"S">>,<<"2">>}]},
			   {<<"description">>,[{<<"S">>,<<"Task TODO">>}]}
			  ]
		]},
		{<<"ScannedCount">>,2}
	]
}.


%ddb_find_mockup(<<"strategy">>, {<<"1">>, 'number'}, {'equal', 'number', [<<"2">>]}) ->
%{ok,[
		%{<<"ConsumedCapacityUnits">>,0.5},
		%{<<"Count">>,1},
		%{<<"Items">>,[
			%[
				%{<<"name">>,[{<<"S">>,<<"My Second Strategy">>}]},
				%{<<"strategy_id">>,[{<<"N">>,<<"2">>}]},
				%{<<"atts">>,[{<<"S">>,<<"some specific stuff 2">>}]},
				%{<<"user_id">>,[{<<"N">>,<<"1">>}]}
			%]
		%]}
	%]
%}.

ddb_find_mockup(<<"task">>, {<<"1">>, 'number'}, {'equal', 'number', [<<"2">>]}) ->
{ok,[
		{<<"ConsumedCapacityUnits">>,0.5},
		{<<"Count">>,1},
		{<<"Items">>,[
			[
				{<<"user_id">>,[{<<"N">>,<<"1">>}]},
				{<<"job_id">>,[{<<"N">>,<<"2">>}]},
				{<<"description">>,[{<<"S">>,<<"My Task">>}]}
			]
		]}
	]
}.


type('string') -> <<"S">>;
type('number') -> <<"N">>;
type(['string']) -> <<"SS">>;
type(['number']) -> <<"NS">>.

ddb_key_value_mockup(HashKeyValue, HashKeyType)
  when is_binary(HashKeyValue),
       is_atom(HashKeyType) ->
    [{<<"Key">>, [{<<"HashKeyElement">>,
                   [{type(HashKeyType), HashKeyValue}]}]}].


ddb_tables() -> {'ok', [<<"user">>,<<"task">>]}.


ddb_describe_table(<<"user">>) ->
	{ok,[{<<"Table">>,
      [{<<"CreationDateTime">>,1360496113.354},
       {<<"ItemCount">>,0},
       {<<"KeySchema">>,
        [{<<"HashKeyElement">>,
          [{<<"AttributeName">>,<<"email">>},
           {<<"AttributeType">>,<<"S">>}]}]},
       {<<"ProvisionedThroughput">>,
        [{<<"NumberOfDecreasesToday">>,0},
         {<<"ReadCapacityUnits">>,1},
         {<<"WriteCapacityUnits">>,1}]},
       {<<"TableName">>,<<"user">>},
       {<<"TableSizeBytes">>,0},
       {<<"TableStatus">>,<<"ACTIVE">>}]}]};

ddb_describe_table(<<"task">>) ->
	{ok,[{<<"Table">>,
      [{<<"CreationDateTime">>,1360496114.746},
       {<<"ItemCount">>,0},
       {<<"KeySchema">>,
        [{<<"HashKeyElement">>,
          [{<<"AttributeName">>,<<"email">>},
           {<<"AttributeType">>,<<"S">>}]},
         {<<"RangeKeyElement">>,
          [{<<"AttributeName">>,<<"job_id">>},
           {<<"AttributeType">>,<<"N">>}]}]},
       {<<"ProvisionedThroughput">>,
        [{<<"NumberOfDecreasesToday">>,0},
         {<<"ReadCapacityUnits">>,1},
         {<<"WriteCapacityUnits">>,1}]},
       {<<"TableName">>,<<"task">>},
       {<<"TableSizeBytes">>,0},
       {<<"TableStatus">>,<<"ACTIVE">>}]}]}.
		

%% mockup key information
table_key_info("user.id")		-> {table,<<"user">>, {aws_base_attribute,<<"id">>,number}, {aws_base_attribute,undefined,undefined}};
table_key_info("user.user_id")	-> {table,<<"user">>, {aws_base_attribute,<<"user_id">>,number}, {aws_base_attribute,undefined,undefined}};
table_key_info("user.user")		-> {table,<<"user">>, {aws_base_attribute,<<"user">>,string}, {aws_base_attribute,undefined,undefined}};
table_key_info("user.email")	-> {table,<<"user">>, {aws_base_attribute,<<"email">>,string}, {aws_base_attribute,undefined,undefined}};

table_key_info("task.id")		-> {table,<<"task">>, {aws_base_attribute,<<"id">>,number}, {aws_base_attribute,undefined,undefined}};

table_key_info("task.user_id, job_id") -> {table,<<"task">>, {aws_base_attribute,<<"user_id">>,number}, {aws_base_attribute,<<"job_id">>,number}};
table_key_info("task.job_id, user_id") -> {table,<<"task">>, {aws_base_attribute,<<"job_id">>,number}, {aws_base_attribute,<<"user_id">>,number}};

table_key_info("task.user, job_id") -> {table,<<"task">>, {aws_base_attribute,<<"user">>,string}, {aws_base_attribute,<<"job_id">>,number}};
table_key_info("task.job_id, user") -> {table,<<"task">>, {aws_base_attribute,<<"job_id">>,number}, {aws_base_attribute,<<"user">>,string}};

table_key_info("task.user, id") -> {table,<<"task">>, {aws_base_attribute,<<"user">>,string}, {aws_base_attribute,<<"id">>,number}};
table_key_info("task.id, user") -> {table,<<"task">>, {aws_base_attribute,<<"id">>,number}, {aws_base_attribute,<<"user">>,string}};

table_key_info("task.email, job_id") -> {table,<<"task">>, {aws_base_attribute,<<"email">>,string}, {aws_base_attribute,<<"job_id">>,number}};
table_key_info("task.job_id, email") -> {table,<<"task">>, {aws_base_attribute,<<"job_id">>,number}, {aws_base_attribute,<<"email">>,string}};

table_key_info("task.email, id") -> {table,<<"task">>, {aws_base_attribute,<<"email">>,string}, {aws_base_attribute,<<"id">>,number}};
table_key_info("task.id, email") -> {table,<<"task">>, {aws_base_attribute,<<"id">>,number}, {aws_base_attribute,<<"email">>,string}};

table_key_info("task.id, job_id") -> {table,<<"task">>, {aws_base_attribute,<<"id">>,number}, {aws_base_attribute,<<"job_id">>,number}};
table_key_info("task.job_id, id") -> {table,<<"task">>, {aws_base_attribute,<<"job_id">>,number}, {aws_base_attribute,<<"id">>,number}};

table_key_info("job.id")		-> {table,<<"job">>, {aws_base_attribute,<<"id">>,number}, {aws_base_attribute,undefined,undefined}};
table_key_info("job.id, user") -> {table,<<"job">>, {aws_base_attribute,<<"id">>,number}, {aws_base_attribute,<<"user">>,string}};
table_key_info("job.time, user") -> {table,<<"job">>, {aws_base_attribute,<<"time">>,number}, {aws_base_attribute,<<"user">>,string}}.


-endif.
