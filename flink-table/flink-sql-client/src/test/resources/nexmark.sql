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

CREATE TABLE datagen
(
    event_type   int,
    person       ROW<
        id  BIGINT,
        name         VARCHAR,
        emailAddress VARCHAR,
        creditCard   VARCHAR,
        city         VARCHAR,
        state        VARCHAR,
        `dateTime`   TIMESTAMP(3),
        extra        VARCHAR>,
    auction      ROW<
        id  BIGINT,
        itemName     VARCHAR,
        description  VARCHAR,
        initialBid   BIGINT,
        reserve      BIGINT,
        `dateTime`   TIMESTAMP(3),
        expires      TIMESTAMP(3),
        seller       BIGINT,
        category     BIGINT,
        extra        VARCHAR>,
    bid          ROW<
        auction  BIGINT,
        bidder       BIGINT,
        price        BIGINT,
        channel      VARCHAR,
        url          VARCHAR,
        `dateTime`   TIMESTAMP(3),
        extra        VARCHAR>,
    `dateTime` AS
        CASE
            WHEN event_type = 0 THEN person.`dateTime`
            WHEN event_type = 1 THEN auction.`dateTime`
            ELSE bid.`dateTime`
        END,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '10'
);
CREATE TEMPORARY VIEW person AS
SELECT person.id,
       person.name,
       person.emailAddress,
       person.creditCard,
       person.city,
       person.state,
       `dateTime`,
       person.extra
FROM datagen
WHERE event_type = 0;

CREATE VIEW auction AS
SELECT auction.id,
       auction.itemName,
       auction.description,
       auction.initialBid,
       auction.reserve,
       `dateTime`,
       auction.expires,
       auction.seller,
       auction.category,
       auction.extra
FROM datagen
WHERE event_type = 1;

CREATE VIEW bid AS
SELECT bid.auction,
       bid.bidder,
       bid.price,
       bid.channel,
       bid.url,
       `dateTime`,
       bid.extra
FROM datagen
WHERE event_type = 2;

CREATE TABLE nexmark_q7
(
    auction    BIGINT,
    bidder     BIGINT,
    price      BIGINT,
    `dateTime` TIMESTAMP(3),
    extra      VARCHAR
) WITH (
      'connector' = 'blackhole'
);

CREATE TABLE nexmark_q8 (id BIGINT, name VARCHAR, stime TIMESTAMP(3)) WITH ('connector' = 'blackhole');

BEGIN STATEMENT SET;
INSERT INTO nexmark_q7
SELECT B.auction, B.price, B.bidder, B.`dateTime`, B.extra
from bid B
         JOIN (SELECT MAX(price) AS maxprice, window_end as `dateTime`
               FROM TABLE(
                       TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
               GROUP BY window_start, window_end) B1
              ON B.price = B1.maxprice
WHERE B.`dateTime` BETWEEN B1.`dateTime` - INTERVAL '10' SECOND AND B1.`dateTime`;

INSERT INTO nexmark_q8
    SELECT
        P.id, P.name, P.starttime
    FROM
        (
            SELECT
                P.id,
                P.name,
                TUMBLE_START(P.dateTime, INTERVAL '10' SECOND) AS starttime,
                TUMBLE_END(P.dateTime, INTERVAL '10' SECOND) AS endtime
            FROM
                person P
            GROUP BY
                P.id,
                P.name,
                TUMBLE(P.dateTime, INTERVAL '10' SECOND)
        ) P
        JOIN (
            SELECT
                A.seller,
                TUMBLE_START(A.dateTime, INTERVAL '10' SECOND) AS starttime,
                TUMBLE_END(A.dateTime, INTERVAL '10' SECOND) AS endtime
            FROM
                auction A
            GROUP BY
                A.seller,
                TUMBLE(A.dateTime, INTERVAL '10' SECOND)
        ) A
            ON P.id = A.seller
                AND P.starttime = A.starttime
                AND P.endtime = A.endtime;
END;

