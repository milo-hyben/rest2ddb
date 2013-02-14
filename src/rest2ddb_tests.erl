%% @author milo hyben
%% @doc REST to DynamoDB interface - tests.

-module(rest2ddb_tests).

%% unit tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

rest2ddb_test_() ->
{
	foreach,
 	fun() ->
			%% mockup ddb
			meck:new(ddb),
            meck:new(ddb_iam),

			%% setup mockup ddb functions
			meck:expect(ddb, tables,
                 fun rest2ddb_utils_mockups:ddb_tables/0
                 ),
			meck:expect(ddb, describe_table,
                 fun rest2ddb_utils_mockups:ddb_describe_table/1
                 ),
			meck:expect(ddb, scan,
                 fun rest2ddb_utils_mockups:ddb_scan_mockup/2
                 ),
			meck:expect(ddb, get,
                 fun rest2ddb_utils_mockups:ddb_get_mockup/3
                 ),
			meck:expect(ddb, key_value,
                 fun rest2ddb_utils_mockups:ddb_key_value_mockup/2
                 ),
			meck:expect(ddb, find,
                 fun rest2ddb_utils_mockups:ddb_find_mockup/3
                 ),
			meck:expect(ddb, credentials,
                 fun (AKey, Secret, Token) -> {'ok'} end
                 ),
			meck:expect(ddb_iam, credentials,
                 fun (AccessKeyId, SecretAccessKey) -> {'ok'} end
                 ),
			meck:expect(ddb_iam, token,
                 fun (DurationInSeconds) -> {'ok', "AccessKeyId", "SecretAccessKey", "Token"} end
                 )
  end,
  fun(_) ->
  		%% unload ddb
      meck:unload(ddb),
      meck:unload(ddb_iam)
  end,
	[

   	{"init_tables",
    	fun() ->
			?assertEqual( ok, rest2ddb:init("AccessKeyId", "SecretAccessKey", 100, "Region")),
			?assertEqual({ok,[<<"user">>,<<"task">>]}, application:get_env('rest2ddb', 'aws_tables')),
			?assertEqual({ok,[rest2ddb_utils_mockups:table_key_info("task.email, job_id")
							 ,rest2ddb_utils_mockups:table_key_info("user.email")]}
						 , application:get_env('rest2ddb', 'aws_tables_details')),
   			?assert(meck:validate(ddb)),
            ?assert(meck:validate(ddb_iam))
    	end
   	},

   	{"get",
    	fun() ->
			%% set environment / global values
            application:set_env('rest2ddb', 'aws_tables', rest2ddb_utils_mockups:environment_mockup('aws_tables')),
			application:set_env('rest2ddb', 'aws_tables_details',
								[
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.user_id, job_id")
								]),

			?assertEqual(	{ok,[[
									{<<"user_id">>,[{<<"N">>,<<"1">>}]},
									{<<"job_id">>,[{<<"N">>,<<"2">>}]},
									{<<"description">>,[{<<"S">>,<<"My Task">>}]}
								]]
							},
							rest2ddb:get(
							[<<"user">>, <<"1">>, <<"task">>, <<"2">>],
							[{<<"fields">>,<<"user_id,job_id,description">>}]
							) ),
   			?assert(meck:validate(ddb)),
            ?assert(meck:validate(ddb_iam))

    	end
   	}

	]
}.

-endif.

