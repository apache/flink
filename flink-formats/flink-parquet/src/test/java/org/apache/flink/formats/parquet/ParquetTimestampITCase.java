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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.planner.runtime.stream.FiniteTestSource;
import org.apache.flink.table.planner.runtime.stream.FsStreamingSinkITCaseBase;
import org.apache.flink.table.planner.runtime.utils.TestSinkUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.parquet.Strings;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Function1;
import scala.collection.Seq;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_TRIGGER;
import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.TIMESTAMP_TIME_UNIT;
import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.UTC_TIMEZONE;
import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.WRITE_INT64_TIMESTAMP;
import static org.junit.Assert.assertEquals;

/** Test int64 timestamp. */
@ExtendWith(ParameterizedTestExtension.class)
public class ParquetTimestampITCase extends FsStreamingSinkITCaseBase {
    @Parameter public static boolean useInt64;

    @Parameter public static String timeunit;

    @Parameter public static boolean timezone;

    @Parameters(name = "useInt64 = {0}, timeunit = {1}, timezone = {2}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {false, "millis", false},
            new Object[] {true, "millis", false},
            new Object[] {true, "micros", false},
            new Object[] {true, "nanos", false},
            new Object[] {true, "millis", true},
            new Object[] {true, "micros", true},
            new Object[] {true, "nanos", true}
        };
    }

    @Override
    public Seq<Row> getData() {
        return JavaScalaConversionUtil.toScala(
                new ArrayList<Row>() {
                    {
                        add(
                                Row.of(
                                        Integer.valueOf(1),
                                        "a",
                                        Timestamp.valueOf("2020-05-03 07:00:00.000000000"),
                                        "05-03-2020",
                                        "07"));
                        add(
                                Row.of(
                                        Integer.valueOf(2),
                                        "p",
                                        Timestamp.valueOf("2020-05-03 08:01:01.111111111"),
                                        "05-03-2020",
                                        "08"));
                        add(
                                Row.of(
                                        Integer.valueOf(3),
                                        "x",
                                        Timestamp.valueOf("2020-05-03 09:02:02.222222222"),
                                        "05-03-2020",
                                        "09"));
                        add(
                                Row.of(
                                        Integer.valueOf(4),
                                        "x",
                                        Timestamp.valueOf("2020-05-03 10:03:03.333333333"),
                                        "05-03-2020",
                                        "10"));
                        add(
                                Row.of(
                                        Integer.valueOf(5),
                                        "x",
                                        Timestamp.valueOf("2020-05-03 11:04:04.444444444"),
                                        "05-03-2020",
                                        "11"));
                    }
                });
    }

    @Override
    public Seq<Row> getData2() {
        return JavaScalaConversionUtil.toScala(
                new ArrayList<Row>() {
                    {
                        add(
                                Row.of(
                                        Integer.valueOf(1),
                                        "a",
                                        Timestamp.valueOf("2020-05-03 07:00:00.000000000"),
                                        "20200503",
                                        "07"));
                        add(
                                Row.of(
                                        Integer.valueOf(2),
                                        "p",
                                        Timestamp.valueOf("2020-05-03 08:01:01.111111111"),
                                        "20200503",
                                        "08"));
                        add(
                                Row.of(
                                        Integer.valueOf(3),
                                        "x",
                                        Timestamp.valueOf("2020-05-03 09:02:02.222222222"),
                                        "20200503",
                                        "09"));
                        add(
                                Row.of(
                                        Integer.valueOf(4),
                                        "x",
                                        Timestamp.valueOf("2020-05-04 10:03:03.333333333"),
                                        "20200504",
                                        "10"));
                        add(
                                Row.of(
                                        Integer.valueOf(5),
                                        "x",
                                        Timestamp.valueOf("2020-05-04 11:04:04.444444444"),
                                        "20200504",
                                        "11"));
                    }
                });
    }

    @Override
    public DataStream<Row> getDataStream2(Function1<Row, Object> fun) {
        return new DataStream<Row>(
                env().getJavaEnv()
                        .addSource(
                                new FiniteTestSource(getData2(), fun),
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            Types.INT,
                                            Types.STRING,
                                            Types.SQL_TIMESTAMP,
                                            Types.STRING,
                                            Types.STRING
                                        },
                                        new String[] {"f0", "f1", "f2", "f3", "f4"})));
    }

    @Override
    public DataStream<Row> getDataStream(Function1<Row, Object> fun) {
        return new DataStream<Row>(
                env().getJavaEnv()
                        .addSource(
                                new FiniteTestSource(getData(), fun),
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            Types.INT,
                                            Types.STRING,
                                            Types.SQL_TIMESTAMP,
                                            Types.STRING,
                                            Types.STRING
                                        },
                                        new String[] {"f0", "f1", "f2", "f3", "f4"})));
    }

    @Override
    public String getDDL(
            String timeExtractorKind,
            String timeExtractorFormatterPattern,
            String timeExtractorPattern,
            String partition,
            String commitTrigger,
            String commitDelay,
            String policy,
            String successFileName) {
        StringBuffer ddl = new StringBuffer("create table sink_table (");
        ddl.append("  a int, ");
        ddl.append(" b string, ");
        ddl.append(" c timestamp(3), ");
        ddl.append(" d string,");
        ddl.append(" e string");
        ddl.append(") ");
        if (!Strings.isNullOrEmpty(partition)) {
            ddl.append("partitioned by ( " + partition + " ) ");
        }
        ddl.append("with ( ");
        ddl.append(" 'connector' = 'filesystem', ");
        ddl.append(" 'path' = '" + resultPath() + "', ");
        ddl.append(
                " '" + PARTITION_TIME_EXTRACTOR_KIND.key() + "' = '" + timeExtractorKind + "', ");
        if (!Strings.isNullOrEmpty(timeExtractorFormatterPattern)) {
            ddl.append(
                    " '"
                            + PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER.key()
                            + "' = '"
                            + timeExtractorFormatterPattern
                            + "', ");
        }
        ddl.append(
                " '"
                        + PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()
                        + "' = '"
                        + timeExtractorPattern
                        + "', ");
        ddl.append(" '" + SINK_PARTITION_COMMIT_TRIGGER.key() + "' = '" + commitTrigger + "', ");
        ddl.append(" '" + SINK_PARTITION_COMMIT_DELAY.key() + "' = '" + commitDelay + "', ");
        ddl.append(" '" + SINK_PARTITION_COMMIT_POLICY_KIND.key() + "' = '" + policy + "', ");
        ddl.append(
                " '"
                        + SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key()
                        + "' = '"
                        + successFileName
                        + "', ");
        ddl.append(" 'format'='parquet', ");
        ddl.append(" 'parquet." + UTC_TIMEZONE.key() + "' = '" + timezone + "', ");
        ddl.append(" 'parquet." + TIMESTAMP_TIME_UNIT.key() + "' = '" + timeunit + "', ");
        ddl.append(" 'parquet." + WRITE_INT64_TIMESTAMP.key() + "' = '" + useInt64 + "'");
        ddl.append(") ");
        return ddl.toString();
    }

    @Override
    public void check(String sqlQuery, Seq<Row> expectedResult) {
        List<Row> result =
                CollectionUtil.iteratorToList(tEnv().sqlQuery(sqlQuery).execute().collect());
        assertEquals(
                JavaScalaConversionUtil.toJava(expectedResult).stream()
                        .map(row -> TestSinkUtil.rowToString(row, TimeZone.getTimeZone("UTC")))
                        .sorted()
                        .collect(Collectors.toList()),
                result.stream()
                        .map(
                                row -> {
                                    row.setField(
                                            2, Timestamp.valueOf((LocalDateTime) row.getField(2)));
                                    return TestSinkUtil.rowToString(
                                            row, TimeZone.getTimeZone("UTC"));
                                })
                        .sorted()
                        .collect(Collectors.toList()));
    }
}
