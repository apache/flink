-- database: presto; groups: tpch; tables: lineitem,supplier
-- CREATE OR REPLACE VIEW revenue AS
--   SELECT
--     l_suppkey AS supplier_no,
--     sum(l_extendedprice * (1 - l_discount)) AS total_revenue
--   FROM
--     lineitem
--   WHERE
--     l_shipdate >= DATE '1996-01-01'
--     AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
-- GROUP BY
--   l_suppkey;
--
-- SELECT
--   s_suppkey,
--   s_name,
--   s_address,
--   s_phone,
--   total_revenue
-- FROM
--   supplier,
--   revenue
-- WHERE
--   s_suppkey = supplier_no
--   AND total_revenue = (
--     SELECT max(total_revenue)
--     FROM
--       revenue
--   )
-- ORDER BY
--   s_suppkey;
-- Blink does not support view

SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  supplier, (
  SELECT
    l_suppkey AS supplier_no,
    sum(l_extendedprice * (1 - l_discount)) AS total_revenue
  FROM
    lineitem
  WHERE
    l_shipdate >= DATE '1996-01-01'
    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
  GROUP BY
    l_suppkey) AS revenue
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT max(total_revenue)
    FROM (
      SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
      FROM
        lineitem
      WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
      GROUP BY
        l_suppkey) AS revenue
  )
ORDER BY
  s_suppkey;
