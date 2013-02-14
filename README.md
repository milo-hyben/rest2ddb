rest2ddb
========

Erlang RESTful interface to Amazon DynamoDB

Purpose of this module is to convert RESTful calls to action on Amazon DynamoDB tables.

It is built as a wrapper around erlang ddb module (git://github.com/Concurix/ddb.git).

Ideally you want to use this module within your favourite Erlang web framework.

Working demo is in progress using elli web framework (git://github.com/knutin/elli.git) and it will be posted on github.


Supported REST operations:


GET

PUT - Not implemented yet

POST - Not implemented yet

PATCH - Not implemented yet

DELETE - Not implemented yet


Prerequisites:

erlang installed

rebar installed

How to compile:

Get dependencies

	rebar get-deps

Compile:

	rebar compile

Run unit tests:

	rebar eunit skip_deps=true

Example of use:


Start erlang in the root project folder

	erl -pa ebin deps/*/ebin

Start required modules

	'ok' = inets:start().
	'ok' = ssl:start().
	'ok' = application:start(ibrowse).


Replace keys with your AWS ones, region is currently being ignored it is set to us-east-1 by default

	'ok' = rest2ddb:init("AccessKeyId", "SecretAccessKey", 900, "us-east-1").


Create table in DynamoDB, and add one record

	{ok, _} = ddb:create_table(<<"user">>, ddb:key_type(<<"user_id">>, 'number'), 1, 1).


Wait approximately 30 seconds to get table created on AWS DynamoDB


You should log into your AWS console and check if table "user" is ACTIVE before proceeding to the next step

	{ok, _} = ddb:put(<<"user">>, [
			{<<"user_id">>, <<"1">>, 'number'},
		  {<<"email">>, <<"test@test.com">>, 'string'},
		  {<<"firstName">>, <<"John">>, 'string'},
		  {<<"lastName">>, <<"Citizen">>, 'string'}
	  ]).

Call this only if you change any of the DynamoDB tables schema

	'ok' = rest2ddb:reload_schema().


Now you are ready to make your first GET call

	%% GET /user/1?fields=firstName,lastName,email
	Url = <<"user/1?fields=firstname,lastname">>.
	Path = rest2ddb:extract_path(Url).
	Qs	= rest2ddb:extract_query(Url).
	rest2ddb:get(Path, Qs).


The same as above, except no filter on fields and pass URL path already splitted into the list

	%% GET /user/1
	rest2ddb:get([<<"user">>,<<"1">>], []).


Get me all users with firstName=John

	%% GET /user?firstName=John
	rest2ddb:get([<<"user">>,<<"1">>], [{<<"firstName">>,<<"John">>}]).

Get me all users whith firstName containing 'J'

	%% GET /user?firstName=*J
	rest2ddb:get([<<"user">>,<<"1">>], [{<<"firstName">>,<<"*J">>}]).


When done, do not forget to remove your table from DynamoDB so you are not charged

	ddb:remove_table(<<"user">>).
