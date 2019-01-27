-- tpch20 using 1395599672 as a seed to the RNG
select
  s.s_name,
  s.s_address
from
  supplier s,
  nation n
where
  s.s_suppkey in (
    select
      ps.ps_suppkey
    from
      partsupp ps
    where
      ps. ps_partkey in (
        select
          p.p_partkey
        from
          part p
        where
          p.p_name like 'antique%'
      )
      and ps.ps_availqty > (
        select
          0.5 * sum(l.l_quantity)
        from
          lineitem l
        where
          l.l_partkey = ps.ps_partkey
          and l.l_suppkey = ps.ps_suppkey
          and l.l_shipdate >= date '1993-01-01'
          and l.l_shipdate < date '1993-01-01' + interval '1' year
      )
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'KENYA'
order by
  s.s_name
