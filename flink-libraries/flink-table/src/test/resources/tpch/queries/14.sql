-- tpch14 using 1395599672 as a seed to the RNG
select
  100.00 * sum(case
    when p.p_type like 'PROMO%'
      then l.l_extendedprice * (1 - l.l_discount)
    else 0
  end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
from
  lineitem l,
  part p
where
  l.l_partkey = p.p_partkey
  and l.l_shipdate >= date '1994-08-01'
  and l.l_shipdate < date '1994-08-01' + interval '1' month
