# materialized_table.q - CREATE/DROP/SHOW/ALTER MATERIALIZED TABLE
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ==========================================================================
# test create source table
# ==========================================================================

CREATE TABLE datagenSource (
  order_id BIGINT,
  order_number VARCHAR(20),
  user_id BIGINT,
  shop_id BIGINT,
  product_id BIGINT,
  status BIGINT,
  order_type BIGINT,
  order_created_at TIMESTAMP(3),
  payment_amount_cents BIGINT
)
WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10'
);
[INFO] Execute statement succeeded.
!info

# ==========================================================================
# test create materialized table without explicit FRESHNESS (uses default)
# REFRESH_MODE = CONTINUOUS to avoid workflow scheduler requirement
# ==========================================================================

CREATE MATERIALIZED TABLE users_shops
PARTITIONED BY (ds)
WITH(
   'connector' = 'blackhole'
)
REFRESH_MODE = CONTINUOUS
AS SELECT
  user_id,
  shop_id,
  ds,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS pv
 FROM (
    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource
 ) AS tmp
 GROUP BY user_id, shop_id, ds;
[INFO] Execute statement succeeded.
!info

# ==========================================================================
# test SHOW CREATE MATERIALIZED TABLE displays enriched FRESHNESS
# ==========================================================================

SHOW CREATE MATERIALIZED TABLE users_shops;
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            result |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CREATE MATERIALIZED TABLE `default_catalog`.`default_database`.`users_shops`
PARTITIONED BY (`ds`)
WITH (
  'connector' = 'blackhole'
)
FRESHNESS = INTERVAL '3' MINUTE
REFRESH_MODE = CONTINUOUS
AS SELECT `tmp`.`user_id`, `tmp`.`shop_id`, `tmp`.`ds`, SUM(`tmp`.`payment_amount_cents`) AS `payed_buy_fee_sum`, SUM(1) AS `pv`
FROM (SELECT `datagenSource`.`user_id`, `datagenSource`.`shop_id`, DATE_FORMAT(`datagenSource`.`order_created_at`, 'yyyy-MM-dd') AS `ds`, `datagenSource`.`payment_amount_cents`
FROM `default_catalog`.`default_database`.`datagenSource` AS `datagenSource`) AS `tmp`
GROUP BY `tmp`.`user_id`, `tmp`.`shop_id`, `tmp`.`ds`
 |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set
!ok

# ==========================================================================
# test create materialized table with explicit FRESHNESS
# ==========================================================================

CREATE MATERIALIZED TABLE users_shops_explicit
PARTITIONED BY (ds)
WITH(
   'connector' = 'blackhole'
)
FRESHNESS = INTERVAL '10' SECOND
REFRESH_MODE = CONTINUOUS
AS SELECT
  user_id,
  shop_id,
  ds,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS pv
 FROM (
    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource
 ) AS tmp
 GROUP BY user_id, shop_id, ds;
[INFO] Execute statement succeeded.
!info

# verify explicit FRESHNESS is preserved
SHOW CREATE MATERIALIZED TABLE users_shops_explicit;
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      result |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CREATE MATERIALIZED TABLE `default_catalog`.`default_database`.`users_shops_explicit`
PARTITIONED BY (`ds`)
WITH (
  'connector' = 'blackhole'
)
FRESHNESS = INTERVAL '10' SECOND
REFRESH_MODE = CONTINUOUS
AS SELECT `tmp`.`user_id`, `tmp`.`shop_id`, `tmp`.`ds`, SUM(`tmp`.`payment_amount_cents`) AS `payed_buy_fee_sum`, SUM(1) AS `pv`
FROM (SELECT `datagenSource`.`user_id`, `datagenSource`.`shop_id`, DATE_FORMAT(`datagenSource`.`order_created_at`, 'yyyy-MM-dd') AS `ds`, `datagenSource`.`payment_amount_cents`
FROM `default_catalog`.`default_database`.`datagenSource` AS `datagenSource`) AS `tmp`
GROUP BY `tmp`.`user_id`, `tmp`.`shop_id`, `tmp`.`ds`
 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set
!ok

# ==========================================================================
# test create materialized table without FRESHNESS or REFRESH_MODE
# ==========================================================================

CREATE MATERIALIZED TABLE users_shops_continuous
WITH(
   'connector' = 'blackhole'
)
AS SELECT
  user_id,
  shop_id,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS pv
 FROM datagenSource
 GROUP BY user_id, shop_id;
[INFO] Execute statement succeeded.
!info

# verify default CONTINUOUS mode with default freshness (3 minutes)
SHOW CREATE MATERIALIZED TABLE users_shops_continuous;
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                result |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CREATE MATERIALIZED TABLE `default_catalog`.`default_database`.`users_shops_continuous`
WITH (
  'connector' = 'blackhole'
)
FRESHNESS = INTERVAL '3' MINUTE
REFRESH_MODE = CONTINUOUS
AS SELECT `datagenSource`.`user_id`, `datagenSource`.`shop_id`, SUM(`datagenSource`.`payment_amount_cents`) AS `payed_buy_fee_sum`, SUM(1) AS `pv`
FROM `default_catalog`.`default_database`.`datagenSource` AS `datagenSource`
GROUP BY `datagenSource`.`user_id`, `datagenSource`.`shop_id`
 |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set
!ok

# ==========================================================================
# test cleanup
# ==========================================================================

DROP MATERIALIZED TABLE users_shops;
[INFO] Execute statement succeeded.
!info

DROP MATERIALIZED TABLE users_shops_explicit;
[INFO] Execute statement succeeded.
!info

DROP MATERIALIZED TABLE users_shops_continuous;
[INFO] Execute statement succeeded.
!info

DROP TABLE datagenSource;
[INFO] Execute statement succeeded.
!info
