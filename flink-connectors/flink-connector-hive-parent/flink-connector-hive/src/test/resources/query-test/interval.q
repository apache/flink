-- SORT_QUERY_RESULTS

select interval_day_time('2 1:2:3'), interval_day_time(cast('2 1:2:3' as varchar(10))), interval_day_time('2 1:2:3') = interval '2 1:2:3' day to second,
interval_day_time('2 1:2:3') > interval '1 1:2:3' day to second, interval '4 1:2:3' day to second <= interval_day_time('2 1:2:3'),
interval_year_month('10-11'), interval_year_month(cast('10-11' as varchar(10))), interval_year_month('10-11') = interval '10-11' year to month,
interval_year_month('10-11') > interval '9-11' year to month, interval '13-11' year to month <= interval_year_month('10-11');

[+I[PT49H2M3S, PT49H2M3S, true, true, false, P131M, P131M, true, true, false]]

select interval '2-2' year to month + interval '3-3' year to month, interval '2-2' year to month - interval '3-3' year to month,
- interval '2-2' year to month + date '2022-02-01', date '2022-02-01' + interval '2-2' year to month, date '2022-01-01' + 1 day + '2' days;

[+I[P65M, P-13M, 2019-12-01, 2024-04-01, 2022-01-04T00:00]]

select interval '99 11:22:33.124' day to second + interval '10 9:8:7.123' day to second, interval '99 11:22:33.124' day to second - interval '10 9:8:7.123' day to second,
- interval '1 2:02:00.123' day to second + date '2022-02-01',
date '2022-02-01' + interval '1 2:02:00.123' day to second, date '2022-01-01' + interval '2' day + interval '1' hour + interval '1' minute + interval '60' second;

[+I[PT2636H30M40.247S, PT2138H14M26.001S, 2022-01-30T21:57:59.877, 2022-02-02T02:02:00.123, 2022-01-03T01:02]]
