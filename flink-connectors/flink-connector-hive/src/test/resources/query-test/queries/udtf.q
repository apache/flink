-- SORT_QUERY_RESULTS

select hiveudtf(ai) from baz;

select explode(array(1,2,3)) from foo;

SELECT explode(map('key1', 100, 'key2', 200)) from src limit 2;
