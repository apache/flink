/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.expressions.utils.ScalarOperatorsTestBase;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static org.apache.flink.table.api.Expressions.lit;

/** Tests for {@code CAST} expression. */
public class TypeConversionsTest extends ScalarOperatorsTestBase {
    @Test
    public void testTimestampWithLocalTimeZoneToString() {
        config().setLocalTimeZone(ZoneOffset.ofHours(2));
        testTableApi(lit(Instant.EPOCH).cast(DataTypes.STRING()), "1970-01-01 02:00:00");
    }

    @Test
    public void testTimestampWithLocalTimeZoneToDate() {
        config().setLocalTimeZone(ZoneOffset.ofHours(4));
        testTableApi(lit(Instant.EPOCH).cast(DataTypes.DATE()), "1970-01-01");
    }

    @Test
    public void testTimestampWithLocalTimeZoneToTime() {
        config().setLocalTimeZone(ZoneOffset.ofHours(4));
        testTableApi(lit(Instant.EPOCH).cast(DataTypes.TIME(0)), "04:00:00");
    }

    @Test
    public void testTimestampWithLocalTimeZoneToTimestamp() {
        config().setLocalTimeZone(ZoneOffset.ofHours(3));
        testTableApi(lit(Instant.EPOCH).cast(DataTypes.TIMESTAMP(0)), "1970-01-01 03:00:00");
    }

    @Test
    public void testStringToTimestampWithLocalTimeZone() {
        config().setLocalTimeZone(ZoneOffset.ofHours(2));
        testTableApi(
                lit("1970-01-01 00:00:00").cast(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)),
                "1970-01-01 00:00:00");

        testSqlApi(
                "cast('1970-01-01 00:00:00' AS TIMESTAMP(0) WITH LOCAL TIME ZONE)",
                "1970-01-01 00:00:00");
    }

    @Test
    public void testTimestampToTimestampWithLocalTimeZone() {
        config().setLocalTimeZone(ZoneOffset.ofHours(2));
        testTableApi(
                lit(LocalDateTime.parse("1970-01-01T00:00:00"))
                        .cast(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)),
                "1970-01-01 00:00:00");

        testSqlApi(
                "cast(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP(0) WITH LOCAL TIME ZONE)",
                "1970-01-01 00:00:00");
    }

    @Test
    public void testTimeToTimestampWithLocalTimeZone() {
        config().setLocalTimeZone(ZoneOffset.ofHours(2));
        testTableApi(
                lit(LocalTime.parse("12:00:00")).cast(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)),
                "1970-01-01 12:00:00");

        testSqlApi(
                "cast(TIME '12:00:00' AS TIMESTAMP(0) WITH LOCAL TIME ZONE)",
                "1970-01-01 12:00:00");
    }

    @Test
    public void testDateToTimestampWithLocalTimeZone() {
        config().setLocalTimeZone(ZoneOffset.ofHours(2));
        testTableApi(
                lit(LocalDate.parse("1970-02-01"))
                        .cast(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)),
                "1970-02-01 00:00:00");

        testSqlApi(
                "cast(DATE '1970-02-01' AS TIMESTAMP(0) WITH LOCAL TIME ZONE)",
                "1970-02-01 00:00:00");
    }
}
