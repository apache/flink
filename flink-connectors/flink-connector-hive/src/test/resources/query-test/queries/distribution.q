explain select x from foo sort by x;

explain select x from foo cluster by x;

explain select x,y from foo distribute by y sort by x desc;

explain select x,y from foo distribute by abs(y);
