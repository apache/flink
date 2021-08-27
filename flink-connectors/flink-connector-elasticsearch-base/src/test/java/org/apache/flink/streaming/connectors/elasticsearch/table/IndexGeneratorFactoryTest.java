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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.ArrayList;
import java.util.List;

/** Tests for {@link IndexGeneratorFactory}. */
public class IndexGeneratorFactoryTest extends TestLogger {

    private TableSchema schema;
    private List<RowData> rows;

    @Before
    public void prepareData() {
        schema =
                new TableSchema.Builder()
                        .field("id", DataTypes.INT())
                        .field("item", DataTypes.STRING())
                        .field("log_ts", DataTypes.BIGINT())
                        .field("log_date", DataTypes.DATE())
                        .field("log_time", DataTypes.TIME())
                        .field("order_timestamp", DataTypes.TIMESTAMP())
                        .field("local_timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                        .field("status", DataTypes.BOOLEAN())
                        .build();

        rows = new ArrayList<>();
        rows.add(
                GenericRowData.of(
                        1,
                        StringData.fromString("apple"),
                        Timestamp.valueOf("2020-03-18 12:12:14").getTime(),
                        (int) LocalDate.parse("2020-03-18").toEpochDay(),
                        (int) (LocalTime.parse("12:12:14").toNanoOfDay() / 1_000_000L),
                        TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-03-18T12:12:14")),
                        TimestampData.fromInstant(Instant.parse("2020-03-18T12:12:14Z")),
                        true));
        rows.add(
                GenericRowData.of(
                        2,
                        StringData.fromString("peanut"),
                        Timestamp.valueOf("2020-03-19 12:12:14").getTime(),
                        (int) LocalDate.parse("2020-03-19").toEpochDay(),
                        (int) (LocalTime.parse("12:22:21").toNanoOfDay() / 1_000_000L),
                        TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-03-19T12:22:14")),
                        TimestampData.fromInstant(Instant.parse("2020-03-19T12:12:14Z")),
                        false));
    }

    @Test
    public void testDynamicIndexFromTimestamp() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "{order_timestamp|yyyy_MM_dd_HH-ss}_index", schema);
        indexGenerator.open();
        Assert.assertEquals("2020_03_18_12-14_index", indexGenerator.generate(rows.get(0)));
        IndexGenerator indexGenerator1 =
                IndexGeneratorFactory.createIndexGenerator(
                        "{order_timestamp|yyyy_MM_dd_HH_mm}_index", schema);
        indexGenerator1.open();
        Assert.assertEquals("2020_03_19_12_22_index", indexGenerator1.generate(rows.get(1)));
    }

    @Test
    public void testDynamicIndexFromDate() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "my-index-{log_date|yyyy/MM/dd}", schema);
        indexGenerator.open();
        Assert.assertEquals("my-index-2020/03/18", indexGenerator.generate(rows.get(0)));
        Assert.assertEquals("my-index-2020/03/19", indexGenerator.generate(rows.get(1)));
    }

    @Test
    public void testDynamicIndexFromTime() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator("my-index-{log_time|HH-mm}", schema);
        indexGenerator.open();
        Assert.assertEquals("my-index-12-12", indexGenerator.generate(rows.get(0)));
        Assert.assertEquals("my-index-12-22", indexGenerator.generate(rows.get(1)));
    }

    @Test
    public void testDynamicIndexDefaultFormat() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator("my-index-{log_time|}", schema);
        indexGenerator.open();
        Assert.assertEquals("my-index-12_12_14", indexGenerator.generate(rows.get(0)));
        Assert.assertEquals("my-index-12_22_21", indexGenerator.generate(rows.get(1)));
    }

    @Test
    public void testDynamicIndexDefaultFormatTimestampWithLocalTimeZone() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator("my-index-{local_timestamp|}", schema);
        indexGenerator.open();
        Assert.assertEquals("my-index-2020_03_18_12_12_14Z", indexGenerator.generate(rows.get(0)));
        Assert.assertEquals("my-index-2020_03_19_12_12_14Z", indexGenerator.generate(rows.get(1)));
    }

    @Test
    public void testGeneralDynamicIndex() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator("index_{item}", schema);
        indexGenerator.open();
        Assert.assertEquals("index_apple", indexGenerator.generate(rows.get(0)));
        Assert.assertEquals("index_peanut", indexGenerator.generate(rows.get(1)));
    }

    @Test
    public void testStaticIndex() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator("my-index", schema);
        indexGenerator.open();
        Assert.assertEquals("my-index", indexGenerator.generate(rows.get(0)));
        Assert.assertEquals("my-index", indexGenerator.generate(rows.get(1)));
    }

    @Test
    public void testUnknownField() {
        String expectedExceptionMsg =
                "Unknown field 'unknown_ts' in index pattern 'my-index-{unknown_ts|yyyy-MM-dd}',"
                        + " please check the field name.";
        try {
            IndexGeneratorFactory.createIndexGenerator("my-index-{unknown_ts|yyyy-MM-dd}", schema);
        } catch (TableException e) {
            Assert.assertEquals(e.getMessage(), expectedExceptionMsg);
        }
    }

    @Test
    public void testUnsupportedTimeType() {
        String expectedExceptionMsg =
                "Unsupported type 'INT' found in Elasticsearch dynamic index field, "
                        + "time-related pattern only support types are: DATE,TIME,TIMESTAMP.";
        try {
            IndexGeneratorFactory.createIndexGenerator("my-index-{id|yyyy-MM-dd}", schema);
        } catch (TableException e) {
            Assert.assertEquals(expectedExceptionMsg, e.getMessage());
        }
    }

    @Test
    public void testUnsupportedMultiParametersType() {
        String expectedExceptionMsg =
                "Chaining dynamic index pattern my-index-{local_date}-{local_time} is not supported,"
                        + " only support single dynamic index pattern.";
        try {
            IndexGeneratorFactory.createIndexGenerator(
                    "my-index-{local_date}-{local_time}", schema);
        } catch (TableException e) {
            Assert.assertEquals(expectedExceptionMsg, e.getMessage());
        }
    }

    @Test
    public void testDynamicIndexUnsupportedFormat() {
        String expectedExceptionMsg = "Unsupported field: HourOfDay";
        try {
            IndexGeneratorFactory.createIndexGenerator(
                    "my-index-{log_date|yyyy/MM/dd HH:mm}", schema);
        } catch (UnsupportedTemporalTypeException e) {
            Assert.assertEquals(expectedExceptionMsg, e.getMessage());
        }
    }

    @Test
    public void testUnsupportedIndexFieldType() {
        String expectedExceptionMsg =
                "Unsupported type BOOLEAN of index field, Supported types are:"
                        + " [DATE, TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE,"
                        + " TIMESTAMP_WITH_LOCAL_TIME_ZONE, VARCHAR, CHAR, TINYINT, INTEGER, BIGINT]";
        try {
            IndexGeneratorFactory.createIndexGenerator("index_{status}", schema);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(expectedExceptionMsg, e.getMessage());
        }
    }
}
