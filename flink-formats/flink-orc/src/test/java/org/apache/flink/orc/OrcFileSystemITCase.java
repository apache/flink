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

package org.apache.flink.orc;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/** ITCase for {@link OrcFileFormatFactory}. */
@RunWith(Parameterized.class)
public class OrcFileSystemITCase extends BatchFileSystemITCaseBase {

    private final boolean configure;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public OrcFileSystemITCase(boolean configure) {
        this.configure = configure;
    }

    @Override
    public String[] formatProperties() {
        List<String> ret = new ArrayList<>();
        ret.add("'format'='orc'");
        if (configure) {
            ret.add("'orc.compress'='snappy'");
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
        Assert.assertNotNull(files);
        Path path = new Path(URI.create(files[0].getAbsolutePath()));

        try {
            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(new Configuration()));
            if (configure) {
                Assert.assertEquals("SNAPPY", reader.getCompressionKind().toString());
            } else {
                Assert.assertEquals("ZLIB", reader.getCompressionKind().toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void before() {
        super.before();
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table orcFilterTable ("
                                        + "x string,"
                                        + "y int,"
                                        + "a int,"
                                        + "b bigint,"
                                        + "c boolean,"
                                        + "d string,"
                                        + "e decimal(8,4),"
                                        + "f date,"
                                        + "g timestamp"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'path' = '%s',"
                                        + "%s)",
                                super.resultPath(), String.join(",\n", formatProperties())));
    }

    @Test
    public void testOrcFilterPushDown() throws ExecutionException, InterruptedException {
        super.tableEnv()
                .executeSql(
                        "insert into orcFilterTable select x, y, a, b, "
                                + "case when y >= 10 then false else true end as c, "
                                + "case when a = 1 then null else x end as d, "
                                + "y * 3.14 as e, "
                                + "date '2020-01-01' as f, "
                                + "timestamp '2020-01-01 05:20:00' as g "
                                + "from originalT")
                .await();

        check(
                "select x, y from orcFilterTable where x = 'x11' and 11 = y",
                Collections.singletonList(Row.of("x11", "11")));

        check(
                "select x, y from orcFilterTable where 4 <= y and y < 8 and x <> 'x6'",
                Arrays.asList(Row.of("x4", "4"), Row.of("x5", "5"), Row.of("x7", "7")));

        check(
                "select x, y from orcFilterTable where x = 'x1' and not y >= 3",
                Collections.singletonList(Row.of("x1", "1")));

        check(
                "select x, y from orcFilterTable where c and y > 2 and y < 4",
                Collections.singletonList(Row.of("x3", "3")));

        check(
                "select x, y from orcFilterTable where d is null and x = 'x5'",
                Collections.singletonList(Row.of("x5", "5")));

        check(
                "select x, y from orcFilterTable where d is not null and y > 25",
                Arrays.asList(Row.of("x26", "26"), Row.of("x27", "27")));

        check(
                "select x, y from orcFilterTable where (d is not null and y > 26) or (d is null and x = 'x3')",
                Arrays.asList(Row.of("x3", "3"), Row.of("x27", "27")));

        check(
                "select x, y from orcFilterTable where e = 3.1400 or x = 'x10'",
                Arrays.asList(Row.of("x1", "1"), Row.of("x10", "10")));

        check(
                "select x, y from orcFilterTable where f = date '2020-01-01' and x = 'x1'",
                Collections.singletonList(Row.of("x1", "1")));

        check(
                "select x, y from orcFilterTable where g = timestamp '2020-01-01 05:20:00' and x = 'x10'",
                Collections.singletonList(Row.of("x10", "10")));
    }

    @Test
    public void testNestedTypes() throws ExecutionException, InterruptedException {
        String path =
                this.getClass().getClassLoader().getResource("test-data-nested.orc").getPath();
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table orcNestedTypesTable ("
                                        + "boolean1 boolean,"
                                        + "byte1 tinyint,"
                                        + "short1 smallint,"
                                        + "int1 int,"
                                        + "long1 bigint,"
                                        + "float1 float,"
                                        + "double1 double,"
                                        + "string1 string,"
                                        + "middle ROW<list ARRAY<ROW<int1 int,string1 string>>>,"
                                        + "list ARRAY<ROW<int1 int,string1 string>>,"
                                        + "map MAP<string,ROW<int1 int,string1 string>>"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'format' = 'orc',"
                                        + "'path' = '%s')",
                                path));

        TableResult tableResult = super.tableEnv().executeSql("SELECT * FROM orcNestedTypesTable");
        List<Row> rows = new ArrayList<>();
        tableResult.collect().forEachRemaining(rows::add);
        assertEquals(
                "+I[false, 1, 1024, 65536, 9223372036854775807, 1.0, -15.0, hi, +I[[+I[1, bye], +I[2, sigh]]], [+I[3, good], +I[4, bad]], {}]",
                rows.get(0).toString());
        assertEquals(
                "+I[true, 100, 2048, 65536, 9223372036854775807, 2.0, -5.0, bye, +I[[+I[1, bye], +I[2, sigh]]], [+I[100000000, cat], +I[-100000, in], +I[1234, hat]], {chani=+I[5, chani], mauddib=+I[1, mauddib]}]",
                rows.get(1).toString());
    }
}
