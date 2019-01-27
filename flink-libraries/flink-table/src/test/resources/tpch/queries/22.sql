-- tpch22 using 1395599672 as a seed to the RNG
select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substring(c_phone from 1 for 2) as cntrycode,
      c_acctbal
    from
      customer c
    where
      substring(c_phone from 1 for 2) in
        ('24', '31', '11', '16', '21', '20', '34')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substring(c_phone from 1 for 2) in
            ('24', '31', '11', '16', '21', '20', '34')
      )
      and not exists (
        select
          *
        from
          orders o
        where
          o.o_custkey = c.c_custkey
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode
