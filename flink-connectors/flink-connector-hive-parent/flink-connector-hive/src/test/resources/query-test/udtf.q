-- SORT_QUERY_RESULTS

select hiveudtf(ai) from baz;

[+I[1], +I[2], +I[3]]

select explode(array(1,2,3)) from foo;

[+I[1], +I[1], +I[1], +I[1], +I[1], +I[2], +I[2], +I[2], +I[2], +I[2], +I[3], +I[3], +I[3], +I[3], +I[3]]

SELECT explode(map('key1', 100, 'key2', 200)) from src limit 2;

[+I[key1, 100], +I[key2, 200]]
