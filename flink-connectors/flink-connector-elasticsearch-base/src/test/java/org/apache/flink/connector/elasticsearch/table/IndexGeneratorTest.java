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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumingThat;

/** Suite tests for {@link IndexGenerator}. */
public class IndexGeneratorTest {

    private static final List<String> fieldNames =
            Arrays.asList(
                    "id",
                    "item",
                    "log_ts",
                    "log_date",
                    "order_timestamp",
                    "log_time",
                    "local_datetime",
                    "local_date",
                    "local_time",
                    "local_timestamp",
                    "note",
                    "status");

    private static final List<DataType> dataTypes =
            Arrays.asList(
                    DataTypes.INT(),
                    DataTypes.STRING(),
                    DataTypes.BIGINT(),
                    DataTypes.DATE().bridgedTo(Date.class),
                    DataTypes.TIMESTAMP().bridgedTo(Timestamp.class),
                    DataTypes.TIME().bridgedTo(Time.class),
                    DataTypes.TIMESTAMP().bridgedTo(LocalDateTime.class),
                    DataTypes.DATE().bridgedTo(LocalDate.class),
                    DataTypes.TIME().bridgedTo(LocalTime.class),
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                    DataTypes.STRING(),
                    DataTypes.BOOLEAN());

    private static final List<RowData> rows =
            Arrays.asList(
                    GenericRowData.of(
                            1,
                            StringData.fromString("apple"),
                            Timestamp.valueOf("2020-03-18 12:12:14").getTime(),
                            (int) Date.valueOf("2020-03-18").toLocalDate().toEpochDay(),
                            TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-18 12:12:14")),
                            (int)
                                    (Time.valueOf("12:12:14").toLocalTime().toNanoOfDay()
                                            / 1_000_000L),
                            TimestampData.fromLocalDateTime(
                                    LocalDateTime.of(2020, 3, 18, 12, 12, 14, 1000)),
                            (int) LocalDate.of(2020, 3, 18).toEpochDay(),
                            (int) (LocalTime.of(12, 13, 14, 2000).toNanoOfDay() / 1_000_000L),
                            TimestampData.fromInstant(
                                    LocalDateTime.of(2020, 3, 18, 3, 12, 14, 1000)
                                            .atZone(ZoneId.of("Asia/Shanghai"))
                                            .toInstant()),
                            "test1",
                            true),
                    GenericRowData.of(
                            2,
                            StringData.fromString("peanut"),
                            Timestamp.valueOf("2020-03-19 12:22:14").getTime(),
                            (int) Date.valueOf("2020-03-19").toLocalDate().toEpochDay(),
                            TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-19 12:22:21")),
                            (int)
                                    (Time.valueOf("12:22:21").toLocalTime().toNanoOfDay()
                                            / 1_000_000L),
                            TimestampData.fromLocalDateTime(
                                    LocalDateTime.of(2020, 3, 19, 12, 22, 14, 1000)),
                            (int) LocalDate.of(2020, 3, 19).toEpochDay(),
                            (int) (LocalTime.of(12, 13, 14, 2000).toNanoOfDay() / 1_000_000L),
                            TimestampData.fromInstant(
                                    LocalDateTime.of(2020, 3, 19, 20, 22, 14, 1000)
                                            .atZone(ZoneId.of("America/Los_Angeles"))
                                            .toInstant()),
                            "test2",
                            false));

    @Test
    public void testDynamicIndexFromTimestampTzUTC() {
        assumingThat(
                ZoneId.systemDefault().equals(ZoneId.of("UTC")),
                () -> {
                    IndexGenerator indexGenerator =
                            IndexGeneratorFactory.createIndexGenerator(
                                    "{local_timestamp|yyyy_MM_dd_HH-ss}_index",
                                    fieldNames,
                                    dataTypes);
                    indexGenerator.open();
                    assertThat(indexGenerator.generate(rows.get(0)))
                            .isEqualTo("2020_03_17_19-14_index");
                    assertThat(indexGenerator.generate(rows.get(1)))
                            .isEqualTo("2020_03_20_03-14_index");
                });
    }

    @Test
    public void testDynamicIndexFromTimestampTzWithSpecificTimezone() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "{local_timestamp|yyyy_MM_dd_HH-ss}_index",
                        fieldNames,
                        dataTypes,
                        ZoneId.of("Europe/Berlin"));
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("2020_03_17_20-14_index");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("2020_03_20_04-14_index");
    }

    @Test
    public void testDynamicIndexFromTimestamp() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "{order_timestamp|yyyy_MM_dd_HH-ss}_index", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("2020_03_18_12-14_index");
        IndexGenerator indexGenerator1 =
                IndexGeneratorFactory.createIndexGenerator(
                        "{order_timestamp|yyyy_MM_dd_HH_mm}_index", fieldNames, dataTypes);
        indexGenerator1.open();
        assertThat(indexGenerator1.generate(rows.get(1))).isEqualTo("2020_03_19_12_22_index");
    }

    @Test
    public void testDynamicIndexFromLocalDateTime() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "{local_datetime|yyyy_MM_dd_HH-ss}_index", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("2020_03_18_12-14_index");
        IndexGenerator indexGenerator1 =
                IndexGeneratorFactory.createIndexGenerator(
                        "{local_datetime|yyyy_MM_dd_HH_mm}_index", fieldNames, dataTypes);
        indexGenerator1.open();
        assertThat(indexGenerator1.generate(rows.get(1))).isEqualTo("2020_03_19_12_22_index");
    }

    @Test
    public void testDynamicIndexFromDate() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "my-index-{log_date|yyyy/MM/dd}", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("my-index-2020/03/18");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("my-index-2020/03/19");
    }

    @Test
    public void testDynamicIndexFromLocalDate() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "my-index-{local_date|yyyy/MM/dd}", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("my-index-2020/03/18");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("my-index-2020/03/19");
    }

    @Test
    public void testDynamicIndexFromTime() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "my-index-{log_time|HH-mm}", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("my-index-12-12");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("my-index-12-22");
    }

    @Test
    public void testDynamicIndexFromLocalTime() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "my-index-{local_time|HH-mm}", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("my-index-12-13");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("my-index-12-13");
    }

    @Test
    public void testDynamicIndexDefaultFormat() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "my-index-{local_time|}", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("my-index-12_13_14");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("my-index-12_13_14");
    }

    @Test
    public void testDynamicIndexFromSystemTime() {
        List<String> supportedUseCases =
                Arrays.asList(
                        "now()",
                        "NOW()",
                        "now( )",
                        "NOW(\t)",
                        "\t NOW( ) \t",
                        "current_timestamp",
                        "CURRENT_TIMESTAMP",
                        "\tcurrent_timestamp\t",
                        " current_timestamp ");

        supportedUseCases.stream()
                .forEach(
                        f -> {
                            DateTimeFormatter dateTimeFormatter =
                                    DateTimeFormatter.ofPattern("yyyy_MM_dd");
                            IndexGenerator indexGenerator =
                                    IndexGeneratorFactory.createIndexGenerator(
                                            String.format("my-index-{%s|yyyy_MM_dd}", f),
                                            fieldNames,
                                            dataTypes);
                            indexGenerator.open();
                            // The date may change during the running of the unit test.
                            // Generate expected index-name based on the current time
                            // before and after calling the generate method.
                            String expectedIndex1 =
                                    "my-index-" + LocalDateTime.now().format(dateTimeFormatter);
                            String actualIndex = indexGenerator.generate(rows.get(1));
                            String expectedIndex2 =
                                    "my-index-" + LocalDateTime.now().format(dateTimeFormatter);
                            assertThat(
                                            actualIndex.equals(expectedIndex1)
                                                    || actualIndex.equals(expectedIndex2))
                                    .isTrue();
                        });

        List<String> invalidUseCases =
                Arrays.asList(
                        "now",
                        "now(",
                        "NOW",
                        "NOW)",
                        "current_timestamp()",
                        "CURRENT_TIMESTAMP()",
                        "CURRENT_timestamp");
        invalidUseCases.stream()
                .forEach(
                        f -> {
                            String expectedExceptionMsg =
                                    String.format(
                                            "Unknown field '%s' in index pattern 'my-index-{%s|yyyy_MM_dd}',"
                                                    + " please check the field name.",
                                            f, f);
                            try {
                                IndexGenerator indexGenerator =
                                        IndexGeneratorFactory.createIndexGenerator(
                                                String.format("my-index-{%s|yyyy_MM_dd}", f),
                                                fieldNames,
                                                dataTypes);
                                indexGenerator.open();
                            } catch (TableException e) {
                                assertThat(e).hasMessage(expectedExceptionMsg);
                            }
                        });
    }

    @Test
    public void testGeneralDynamicIndex() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator("index_{item}", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("index_apple");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("index_peanut");
    }

    @Test
    public void testStaticIndex() {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator("my-index", fieldNames, dataTypes);
        indexGenerator.open();
        assertThat(indexGenerator.generate(rows.get(0))).isEqualTo("my-index");
        assertThat(indexGenerator.generate(rows.get(1))).isEqualTo("my-index");
    }

    @Test
    public void testUnknownField() {
        String expectedExceptionMsg =
                "Unknown field 'unknown_ts' in index pattern 'my-index-{unknown_ts|yyyy-MM-dd}',"
                        + " please check the field name.";
        assertThatThrownBy(
                        () ->
                                IndexGeneratorFactory.createIndexGenerator(
                                        "my-index-{unknown_ts|yyyy-MM-dd}", fieldNames, dataTypes))
                .isInstanceOf(TableException.class)
                .hasMessage(expectedExceptionMsg);
    }

    @Test
    public void testUnsupportedTimeType() {
        String expectedExceptionMsg =
                "Unsupported type 'INT' found in Elasticsearch dynamic index field, "
                        + "time-related pattern only support types are: DATE,TIME,TIMESTAMP.";
        assertThatThrownBy(
                        () ->
                                IndexGeneratorFactory.createIndexGenerator(
                                        "my-index-{id|yyyy-MM-dd}", fieldNames, dataTypes))
                .isInstanceOf(TableException.class)
                .hasMessage(expectedExceptionMsg);
    }

    @Test
    public void testUnsupportedMultiParametersType() {
        String expectedExceptionMsg =
                "Chaining dynamic index pattern my-index-{local_date}-{local_time} is not supported,"
                        + " only support single dynamic index pattern.";
        assertThatThrownBy(
                        () ->
                                IndexGeneratorFactory.createIndexGenerator(
                                        "my-index-{local_date}-{local_time}",
                                        fieldNames,
                                        dataTypes))
                .isInstanceOf(TableException.class)
                .hasMessage(expectedExceptionMsg);
    }

    @Test
    public void testUnsupportedIndexFieldType() {
        String expectedExceptionMsg =
                "Unsupported type BOOLEAN of index field, Supported types are:"
                        + " [DATE, TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE,"
                        + " TIMESTAMP_WITH_LOCAL_TIME_ZONE, VARCHAR, CHAR, TINYINT, INTEGER, BIGINT]";
        assertThatThrownBy(
                        () ->
                                IndexGeneratorFactory.createIndexGenerator(
                                        "index_{status}", fieldNames, dataTypes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedExceptionMsg);
    }
}
