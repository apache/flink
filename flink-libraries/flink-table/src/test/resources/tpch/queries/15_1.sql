-- tpch15 using 1395599672 as a seed to the RNG

select
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  r.total_revenue
from
  supplier s,
  (
  select
      l_suppkey as supplier_no,
      sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
      lineitem
    where
      l_shipdate >= date '1993-05-01'
      and l_shipdate < date '1993-05-01' + interval '3' month
    group by
      l_suppkey
  ) as r
where
  s.s_suppkey = r.supplier_no
  and r.total_revenue = (
    select
      max(total_revenue)
    from
    (
      select
          l_suppkey as supplier_no,
          sum(l_extendedprice * (1 - l_discount)) as total_revenue
        from
          lineitem
        where
          l_shipdate >= date '1993-05-01'
          and l_shipdate < date '1993-05-01' + interval '3' month
        group by
          l_suppkey
    ) as revenue0
  )
order by
  s.s_suppkey

