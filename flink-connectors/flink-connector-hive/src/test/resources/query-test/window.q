select row_number() over(), x, y from foo;

[+I[1, 1, 1], +I[2, 2, 2], +I[3, 3, 3], +I[4, 4, 4], +I[5, 5, 5]]

select row_number() over(order by x desc), x, y from foo;

[+I[1, 5, 5], +I[2, 4, 4], +I[3, 3, 3], +I[4, 2, 2], +I[5, 1, 1]]
