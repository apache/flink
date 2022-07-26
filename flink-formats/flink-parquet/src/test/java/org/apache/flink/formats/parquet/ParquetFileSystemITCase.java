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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link ParquetFileFormatFactory}. */
@RunWith(Parameterized.class)
public class ParquetFileSystemITCase extends BatchFileSystemITCaseBase {

    private final boolean configure;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public ParquetFileSystemITCase(boolean configure) {
        this.configure = configure;
    }

    @Override
    public void before() {
        super.before();
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table parquetLimitTable ("
                                        + "x string,"
                                        + "y int,"
                                        + "a int"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'path' = '%s',"
                                        + "%s)",
                                super.resultPath(), String.join(",\n", formatProperties())));
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table parquetFilterTable ("
                                        + "a tinyint,"
                                        + "b smallint,"
                                        + "c int,"
                                        + "d bigint,"
                                        + "e float,"
                                        + "f double,"
                                        + "g boolean,"
                                        + "h string,"
                                        + "i varbinary,"
                                        + "j decimal(5,1),"
                                        + "k date,"
                                        + "l time,"
                                        + "m timestamp,"
                                        + "o timestamp_ltz"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'path' = '%s',"
                                        + "%s)",
                                super.resultPath(), String.join(",\n", formatProperties())));
    }

    @Override
    public String[] formatProperties() {
        List<String> ret = new ArrayList<>();
        ret.add("'format'='parquet'");
        if (configure) {
            ret.add("'parquet.utc-timezone'='true'");
            ret.add("'parquet.compression'='gzip'");
        }
        return ret.toArray(new String[0]);
    }

    @Override
    public void testNonPartition() {
        super.testNonPartition();

        // test configure success
        File directory = new File(URI.create(resultPath()).getPath());
        File[] files =
                directory.listFiles((dir, name) -> !name.startsWith(".") && !name.startsWith("_"));
        assertThat(files).isNotNull();
        Path path = new Path(URI.create(files[0].getAbsolutePath()));

        try {
            ParquetMetadata footer =
                    readFooter(new Configuration(), path, range(0, Long.MAX_VALUE));
            if (configure) {
                assertThat(footer.getBlocks().get(0).getColumns().get(0).getCodec().toString())
                        .isEqualTo("GZIP");
            } else {
                assertThat(footer.getBlocks().get(0).getColumns().get(0).getCodec().toString())
                        .isEqualTo("SNAPPY");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLimitableBulkFormat() throws ExecutionException, InterruptedException {
        super.tableEnv()
                .executeSql(
                        "insert into parquetLimitTable select x, y, "
                                + "1 as a "
                                + "from originalT")
                .await();
        TableResult tableResult1 =
                super.tableEnv().executeSql("SELECT * FROM parquetLimitTable limit 5");
        List<Row> rows1 = CollectionUtil.iteratorToList(tableResult1.collect());
        assertThat(rows1).hasSize(5);

        check(
                "select a from parquetLimitTable limit 5",
                Arrays.asList(Row.of(1), Row.of(1), Row.of(1), Row.of(1), Row.of(1)));
    }

    @Test
    public void testParquetFilterPushDown() throws ExecutionException, InterruptedException {
        final LocalDateTime localDateTime = LocalDateTime.now();
        final DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
        final DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
        final DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        final Table table =
                super.tableEnv()
                        .fromValues(getTestRows(localDateTime))
                        .as("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "o");
        super.tableEnv().createTemporaryView("parquetSource", table);

        super.tableEnv()
                .executeSql("INSERT INTO parquetFilterTable SELECT * from parquetSource")
                .await();

        // test tinyint/smallint/int/bigint
        check(
                "select a from parquetFilterTable where a >= 0 and b >= 2 and c >= 6 and (d <= 7 or d = 9)",
                Arrays.asList(Row.of(6), Row.of(7), Row.of(9)));

        // test float
        check(
                "select e from parquetFilterTable where e >= 8 and e <> 9",
                Collections.singletonList(Row.of(8F)));

        // test double
        check(
                "select f from parquetFilterTable where f <= 1 and e <> 0",
                Collections.singletonList(Row.of(1D)));

        // test boolean
        check(
                "select a, g from parquetFilterTable where g and a >= 6",
                Arrays.asList(Row.of(6, true), Row.of(8, true)));

        // test string
        check(
                "select a, h from parquetFilterTable where h = '6' ",
                Collections.singletonList(Row.of(6, "6")));

        // test decimal
        check(
                "select a, j from parquetFilterTable where j >= 8.0 and j <= 9.0  ",
                Arrays.asList(Row.of(8, 8.0), Row.of(9, 9.0)));

        // test date
        String sql =
                String.format(
                        "select a, k from parquetFilterTable where k > '%s' and k <= '%s'",
                        localDateTime.plusDays(7).format(dateFormatter),
                        localDateTime.plusDays(9).format(dateFormatter));
        check(
                sql,
                Arrays.asList(
                        Row.of(8, localDateTime.plusDays(8).toLocalDate()),
                        Row.of(9, localDateTime.plusDays(9).toLocalDate())));

        // test time
        sql =
                String.format(
                        "select a, l from parquetFilterTable where l >= '%s' and l < '%s' and k > '%s'",
                        localDateTime.plusSeconds(0).format(timeFormatter),
                        localDateTime.plusSeconds(2).format(timeFormatter),
                        localDateTime.plusDays(0).format(dateFormatter));
        check(
                sql,
                Collections.singletonList(Row.of(1, localDateTime.plusSeconds(1).toLocalTime())));

        // test timestamp
        sql =
                String.format(
                        "select a, m from parquetFilterTable where m >= timestamp '%s' or m < timestamp '%s' ",
                        localDateTime.plusSeconds(9).format(dateTimeFormatter),
                        localDateTime.plusSeconds(1).format(dateTimeFormatter));
        check(
                sql,
                Arrays.asList(
                        Row.of(9, localDateTime.plusSeconds(9)),
                        Row.of(0, localDateTime.plusSeconds(0))));

        // test timestamp_ltz
        sql =
                String.format(
                        "select a, o from parquetFilterTable where o >= TO_TIMESTAMP_LTZ (%d, 0) or o < TO_TIMESTAMP_LTZ(%d,0) ",
                        localDateTime.plusSeconds(9).atOffset(ZoneOffset.UTC).toEpochSecond(),
                        localDateTime.plusSeconds(1).atOffset(ZoneOffset.UTC).toEpochSecond());

        check(
                sql,
                Arrays.asList(
                        Row.of(
                                9,
                                localDateTime
                                        .plusSeconds(9)
                                        .atZone(ZoneId.systemDefault())
                                        .toInstant()),
                        Row.of(
                                0,
                                localDateTime
                                        .plusSeconds(0)
                                        .atZone(ZoneId.systemDefault())
                                        .toInstant())));

        // test not and is not null
        check(
                "select a from parquetFilterTable where not a < 9 and a is not null",
                Collections.singletonList(Row.of(9)));

        // test not and not is null
        check(
                "select a from parquetFilterTable where not a < 9 and not a is null",
                Collections.singletonList(Row.of(9)));

        // test true and is not true

        // test false and is not false
    }

    @Test
    public void t1() throws Exception {
        final LocalDateTime localDateTime = LocalDateTime.now();
        final DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
        final DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
        final DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        final Table table =
                super.tableEnv()
                        .fromValues(getTestRows(localDateTime))
                        .as("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "o");
        super.tableEnv().createTemporaryView("parquetSource", table);

        super.tableEnv()
                .executeSql("INSERT INTO parquetFilterTable SELECT * from parquetSource")
                .await();

        // test decimal
        check(
                "select a, j from parquetFilterTable where j >= 8.0 and j <= 9.0  ",
                Arrays.asList(Row.of(8, 8.0), Row.of(9, 9.0)));

        // test boolean
        check(
                "select a, g from parquetFilterTable where g and a >= 6",
                Arrays.asList(Row.of(6, true), Row.of(8, true)));
    }

    private List<Row> getTestRows(LocalDateTime localDateTime) {
        int n = 10;
        List<Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Integer v = i;
            rows.add(
                    Row.of(
                            v.byteValue(),
                            v.shortValue(),
                            v,
                            v.longValue(),
                            v.floatValue(),
                            v.doubleValue(),
                            v % 2 == 0,
                            String.valueOf(v),
                            String.valueOf(v).getBytes(StandardCharsets.UTF_8),
                            BigDecimal.valueOf(i * 1.0),
                            localDateTime.plusDays(v).toLocalDate(),
                            localDateTime.plusSeconds(v).toLocalTime(),
                            localDateTime.plusSeconds(v),
                            localDateTime
                                    .plusSeconds(v)
                                    .atZone(ZoneId.systemDefault())
                                    .toInstant()));
        }
        return rows;
    }
}
