-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
-- we don't support view

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
