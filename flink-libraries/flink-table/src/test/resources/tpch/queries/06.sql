-- tpch6 using 1395599672 as a seed to the RNG

select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1997-01-01'
  and l_shipdate < date '1997-01-01' + interval '1' year
  and
  l_discount between 0.03 - 0.01 and 0.03 + 0.01
  and l_quantity < 24
