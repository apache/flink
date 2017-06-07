-- tpch13 using 1395599672 as a seed to the RNG
select
  c_count,
  count(*) as custdist
from
  (
    select
      c.c_custkey,
      count(o.o_orderkey)
    from
      customer c
      left outer join orders o
        on c.c_custkey = o.o_custkey
        and o.o_comment not like '%special%requests%'
    group by
      c.c_custkey
  ) as orders (c_custkey, c_count)
group by
  c_count
order by
  custdist desc,
  c_count desc
