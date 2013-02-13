%% @author milo
%% @doc @todo Add description to bto_resource_handler.

-module(rest2ddb_utils_tests).

%% unit tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


match_resource_to_keys_test_() ->
{
	foreach,
 	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables', rest2ddb_utils_mockups:environment_mockup('aws_tables')),

			%% mockup ddb
			meck:new(ddb),

			meck:expect(ddb, key_value,
                 fun rest2ddb_utils_mockups:ddb_key_value_mockup/2
                 )
  end,
  fun(_) ->
  		%% unload ddb
      meck:unload(ddb)
  end,
	[
   	{"match_resource_to_keys for user.id -> task.user_id, job_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[ 
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.user_id, job_id")
								]),
								
			?assertEqual( [{"task",[{"user_id","1"}, {"job_id","5"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.id -> task.job_id, user_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[	
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.job_id, user_id")
								]),
								
			?assertEqual( [{"task",[{"job_id","5"}, {"user_id","1"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
	{"match_resource_to_keys for user.id -> task.user, job_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.user, job_id")
								]),
								
			?assertEqual( [{"task",[{"user","1"}, {"job_id","5"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.id -> task.job_id, user",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.job_id, user")
								]),
								
			?assertEqual( [{"task",[{"job_id","5"}, {"user","1"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

	{"match_resource_to_keys for user.id -> task.user, id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[	
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.user, id")
								]),
								
			?assertEqual( [{"task",[{"user","1"}, {"id","5"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.id -> task.id, user",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.id, user")
								]),
								
			?assertEqual( [{"task",[{"id","5"}, {"user","1"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
	{"match_resource_to_keys for user.user_id -> task.user_id, job_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables', [<<"user">>,<<"task">>]),
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.user_id"),
									rest2ddb_utils_mockups:table_key_info("task.user_id, job_id")
								]),
								
			?assertEqual( [{"task",[{"user_id","1"}, {"job_id","5"}]}, {"user",[{"user_id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.user_id -> task.job_id, user_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.user_id"),
									rest2ddb_utils_mockups:table_key_info("task.job_id, user_id")
								]),
								
			?assertEqual( [{"task",[{"job_id","5"}, {"user_id","1"}]}, {"user",[{"user_id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
	{"match_resource_to_keys for user.user -> task.user_id, job_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables', [<<"user">>,<<"task">>]),
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.user"),
									rest2ddb_utils_mockups:table_key_info("task.user_id, job_id")
								]),
								
			?assertEqual( [{"task",[{"user_id","1"}, {"job_id","5"}]}, {"user",[{"user","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.user -> task.job_id, user_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.user"),
									rest2ddb_utils_mockups:table_key_info("task.job_id, user_id")
								]),
								
			?assertEqual( [{"task",[{"job_id","5"}, {"user_id","1"}]}, {"user",[{"user","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
	{"match_resource_to_keys for user.email -> task.user_id, job_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables', [<<"user">>,<<"task">>]),
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.email, job_id")
								]),
								
			?assertEqual( [{"task",[{"email","1"}, {"job_id","5"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.email -> task.job_id, user_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.job_id, email")
								]),
								
			?assertEqual( [{"task",[{"job_id","5"}, {"email","1"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
	{"match_resource_to_keys for user.email -> task.user_id, id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables', [<<"user">>,<<"task">>]),
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.email, id")
								]),
								
			?assertEqual( [{"task",[{"email","1"}, {"id","5"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.email -> task.id, user_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.id, email")
								]),
								
			?assertEqual( [{"task",[{"id","5"}, {"email","1"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},

   	{"match_resource_to_keys for user.email -> task.id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.id")
								]),
								
			?assertEqual( [{"task",[{"id","5"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
   	
   	{"match_resource_to_keys for user.email -> task.id, job_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.id, job_id")
								]),
								
			?assertEqual( [{"task",[{"id","5"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
   	
   	{"match_resource_to_keys for user.email -> task.id, job_id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.job_id, id")
								]),
								
			?assertEqual( [{"task",[{"job_id","5"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))

    	end
   	},
   	
   	{"match_resource_to_keys for user.id -> task.user_id, job_id -> job.id",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.user_id, job_id"),
									rest2ddb_utils_mockups:table_key_info("job.id")
								]),
								
			?assertEqual( [{"job",[{"id","5"}]}, {"task",[{"user_id","1"}, {"job_id","5"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>, <<"job">>], [], []))

    	end
   	},
   	
   	{"match_resource_to_keys for user.id -> task.user_id, job_id -> job.id, user",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.user_id, job_id"),
									rest2ddb_utils_mockups:table_key_info("job.id, user")
								]),
								
			?assertEqual( [{"job",[{"id","5"}, {"user", "1"}]}, {"task",[{"user_id","1"}, {"job_id","5"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>, <<"job">>], [], []))

    	end
   	},
   	
   	{"match_resource_to_keys for user.id -> task.user, id -> job.time, user",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.id"),
									rest2ddb_utils_mockups:table_key_info("task.user, id"),
									rest2ddb_utils_mockups:table_key_info("job.time, user")
								]),
								
			?assertEqual( [{"job",[{"user", "1"}]}, {"task",[{"user","1"}, {"id","5"}]}, {"user",[{"id","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>, <<"job">>], [], []))

    	end
   	},
   	
   	{"match_resource_to_keys for user.id -> task.user, id -> job.time, user",
    	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables_details', 
								[
									rest2ddb_utils_mockups:table_key_info("user.email"),
									rest2ddb_utils_mockups:table_key_info("task.id, job_id"),
									rest2ddb_utils_mockups:table_key_info("job.time, user")
								]),
								
			?assertEqual( [{"job",[{"user", "1"}]}, {"task",[{"id","5"}]}, {"user",[{"email","1"}]}]
         				 , rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>, <<"job">>], [], []))

    	end
   	},
   	
   	{"match_resource_to_keys",
    	fun() ->
			application:set_env('rest2ddb', 'aws_tables_details', rest2ddb_utils_mockups:environment_mockup('aws_tables_details')),
			?assertEqual( [{"job",[{"id","2"}]}, {"user_job",[{"user","1"}, {"job","2"}]}, {"user",[{"id","1"}]}]
					, rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"job">>, <<"2">>], [], [])),
			?assertEqual( {error,[]}
					, rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"job">>, <<"1">>, <<"notExistingTable">>], [], [])),
			?assertEqual( {error,[]}
					, rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"job">>, <<"1">>, <<"notAResource">>, <<"11">>], [], [])),
			?assertEqual( {error,[]}
					, rest2ddb_utils:match_resource_to_keys([<<"notAResource">>, <<"11">>], [], [])),
			?assertEqual( [{"task",[{"user_id","1"},{"task_id","5"}]},{"user",[{"id","1"}]}]
					, rest2ddb_utils:match_resource_to_keys([<<"user">>, <<"1">>, <<"task">>, <<"5">>], [], []))
    	end
   	}

	]
}.


find_resource_test_() ->
{
	foreach,
 	fun() ->
			%% set environment / global values
			application:set_env('rest2ddb', 'aws_tables', rest2ddb_utils_mockups:environment_mockup('aws_tables')),

			%% mockup ddb
			meck:new(ddb),

			%% setup mockup ddb functions
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
					 )                 
	end,
	fun(_) ->
		%% unload ddb
	  meck:unload(ddb)
	end,
	[
	
	{"find resource",
    	fun() ->
			application:set_env('rest2ddb', 'aws_tables_details', rest2ddb_utils_mockups:environment_mockup('aws_tables_details')),
			?assertEqual(	{ok,[[
									{<<"name">>,[{<<"S">>,<<"John">>}]},
									{<<"user_id">>,[{<<"N">>,<<"1">>}]},
									{<<"role">>,[{<<"S">>,<<"user">>}]},
									{<<"email">>,[{<<"S">>,<<"test@test.com">>}]},
									{<<"surname">>,[{<<"S">>,<<"Citizen">>}]}
								]]
							},
							rest2ddb_utils:find_resource([<<"user">>, <<"1">>], []) ),
			
			?assertEqual(	{ok,[[
									{<<"name">>,[{<<"S">>,<<"John">>}]},
									{<<"user_id">>,[{<<"N">>,<<"1">>}]}
								]]
							},
							rest2ddb_utils:find_resource(
							[<<"user">>, <<"1">>],
							[{<<"fields">>,<<"name,user_id">>}]
							) ),

			?assertEqual(	{ok,[[
									{<<"name">>,[{<<"S">>,<<"John">>}]},
									{<<"user_id">>,[{<<"N">>,<<"1">>}]}
								]]
							},
							rest2ddb_utils:find_resource(
							[<<"user">>],
							[{<<"fields">>,<<"name,user_id">>}, {<<"user_id">>,<<"1">>}]
							) ),

			?assertEqual(	{ok,[[
									{<<"name">>,[{<<"S">>,<<"John">>}]},
									{<<"user_id">>,[{<<"N">>,<<"1">>}]}
								]]
							},
							rest2ddb_utils:find_resource(
							[<<"user">>],
							[{<<"fields">>,<<"name,user_id">>}, {<<"name">>,<<"John">>}]
							) ),

			?assertEqual(	{ok,[[
									{<<"name">>,[{<<"S">>,<<"John">>}]},
									{<<"user_id">>,[{<<"N">>,<<"1">>}]},
									{<<"surname">>,[{<<"S">>,<<"Citizen1">>}]}
								]]
							},
							rest2ddb_utils:find_resource(
							[<<"user">>],
							[{<<"fields">>,<<"name,user_id,surname">>}, {<<"name">>,<<"John">>}, {<<"surname">>,<<"*C">>}]
							) ),

			?assertEqual(	{ok,[[
									{<<"user_id">>,[{<<"N">>,<<"1">>}]},
									{<<"task_id">>,[{<<"S">>,<<"2">>}]},
									{<<"description">>,[{<<"S">>,<<"Task TODO">>}]}
								]]
							},
							rest2ddb_utils:find_resource(
							[<<"user">>, <<"1">>, <<"task">>],
							[{<<"fields">>,<<"user_id,task_id,description">>}, {<<"description">>,<<"*T">>}]
							) ),

			?assertEqual(	{ok,[[
									{<<"user_id">>,[{<<"N">>,<<"1">>}]},
									{<<"task_id">>,[{<<"S">>,<<"2">>}]},
									{<<"description">>,[{<<"S">>,<<"Task TODO">>}]}
								]]
							},
							rest2ddb_utils:find_resource(
							[<<"user">>, <<"1">>, <<"task">>],
							[{<<"fields">>,<<"user_id,task_id,description">>}, {<<"task_id">>,<<"2">>}]
							) ),
   				?assert(meck:validate(ddb))
			end
	},
		
	
   	{"find_resource",
    	fun() ->
			%% set environment / global values
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
							rest2ddb_utils:find_resource(
							[<<"user">>, <<"1">>, <<"task">>, <<"2">>],
							[{<<"fields">>,<<"user_id,job_id,description">>}]
							) ),
   			?assert(meck:validate(ddb))

    	end
   	}
	]
}.

-endif.

