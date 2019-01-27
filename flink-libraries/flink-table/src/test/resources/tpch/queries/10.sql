-- tpch10 using 1395599672 as a seed to the RNG
select
  c.c_custkey,
  c.c_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
  c.c_acctbal,
  n.n_name,
  c.c_address,
  c.c_phone,
  c.c_comment
from
  customer c,
  orders o,
  lineitem l,
  nation n
where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and o.o_orderdate >= date '1994-03-01'
  and o.o_orderdate < date '1994-03-01' + interval '3' month
  and l.l_returnflag = 'R'
  and c.c_nationkey = n.n_nationkey
group by
  c.c_custkey,
  c.c_name,
  c.c_acctbal,
  c.c_phone,
  n.n_name,
  c.c_address,
  c.c_comment
order by
  revenue desc
limit 20
