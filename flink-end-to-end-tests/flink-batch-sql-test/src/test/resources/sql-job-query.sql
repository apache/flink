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

INSERT INTO sinkTable
SELECT
    SUM(correct) AS correct,
    TUMBLE_START(rowtime, INTERVAL '20' SECOND) AS rowtime
FROM (
    -- join query
    SELECT
        t1.key,
        t2.rowtime AS rowtime,
        t2.correct,
        t2.wStart
    FROM table2 t1, (
        -- tumble query
        SELECT
            key,
            CASE SUM(cnt) / COUNT(*) WHEN 101 THEN 1 WHEN -1 THEN NULL ELSE 99 END AS correct,
            TUMBLE_START(rowtime, INTERVAL '10' SECOND) AS wStart,
            TUMBLE_ROWTIME(rowtime, INTERVAL '10' SECOND) AS rowtime
        FROM (
            -- over query
            SELECT key, rowtime, 42 AS cnt FROM table1
        )
        -- tumble query
        WHERE rowtime > TIMESTAMP '1970-01-01 00:00:01'
        GROUP BY key, TUMBLE(rowtime, INTERVAL '10' SECOND)
    ) t2
    -- join query
    WHERE
        t1.key = t2.key AND
        t1.rowtime BETWEEN t2.rowtime AND t2.rowtime + INTERVAL '10' SECOND
)
GROUP BY TUMBLE(rowtime, INTERVAL '20' SECOND)
