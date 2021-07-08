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

import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/** ITCase for {@link OrcFileFormatFactory}. */
@RunWith(Parameterized.class)
public class OrcFileSystemITCase extends BatchFileSystemITCaseBase {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

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
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table orcLimitTable ("
                                        + "x string,"
                                        + "y int,"
                                        + "a int"
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
    public void testNestedTypes() throws Exception {
        String path = initNestedTypesFile(initNestedTypesData());
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table orcNestedTypesTable ("
                                        + "_col0 string,"
                                        + "_col1 int,"
                                        + "_col2 ARRAY<ROW<_col2_col0 string>>,"
                                        + "_col3 MAP<string,ROW<_col3_col0 string,_col3_col1 timestamp>>"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'format' = 'orc',"
                                        + "'path' = '%s')",
                                path));

        TableResult tableResult = super.tableEnv().executeSql("SELECT * FROM orcNestedTypesTable");
        List<Row> rows = CollectionUtil.iteratorToList(tableResult.collect());
        assertEquals(4, rows.size());
        assertEquals(
                "+I[_col_0_string_1, 1, [+I[_col_2_row_0_string_1], +I[_col_2_row_1_string_1]], {_col_3_map_key_1=+I[_col_3_map_value_string_1, "
                        + new Timestamp(3600000).toLocalDateTime()
                        + "]}]",
                rows.get(0).toString());
        assertEquals("+I[_col_0_string_2, 2, null, null]", rows.get(1).toString());
        assertEquals("+I[_col_0_string_3, 3, [], {}]", rows.get(2).toString());
        assertEquals("+I[_col_0_string_4, 4, [], {null=null}]", rows.get(3).toString());
    }

    private List<RowData> initNestedTypesData() {
        List<RowData> data = new ArrayList<>(3);
        {
            GenericRowData rowData = new GenericRowData(4);

            rowData.setField(0, new BinaryStringData("_col_0_string_1"));

            rowData.setField(1, 1);

            GenericRowData arrayValue1 = new GenericRowData(1);
            arrayValue1.setField(0, new BinaryStringData("_col_2_row_0_string_1"));
            GenericRowData arrayValue2 = new GenericRowData(1);
            arrayValue2.setField(0, new BinaryStringData("_col_2_row_1_string_1"));
            GenericArrayData arrayData =
                    new GenericArrayData(new Object[] {arrayValue1, arrayValue2});
            rowData.setField(2, arrayData);

            GenericRowData mapValue1 = new GenericRowData(2);
            mapValue1.setField(0, new BinaryStringData(("_col_3_map_value_string_1")));
            mapValue1.setField(1, TimestampData.fromTimestamp(new Timestamp(3600000)));
            Map<StringData, RowData> mapDataMap = new HashMap<>();
            mapDataMap.put(new BinaryStringData("_col_3_map_key_1"), mapValue1);
            GenericMapData mapData = new GenericMapData(mapDataMap);
            rowData.setField(3, mapData);

            data.add(rowData);
        }
        {
            GenericRowData rowData = new GenericRowData(4);
            rowData.setField(0, new BinaryStringData("_col_0_string_2"));
            rowData.setField(1, 2);
            rowData.setField(2, null);
            rowData.setField(3, null);
            data.add(rowData);
        }
        {
            GenericRowData rowData = new GenericRowData(4);
            rowData.setField(0, new BinaryStringData("_col_0_string_3"));
            rowData.setField(1, 3);
            rowData.setField(2, new GenericArrayData(new Object[0]));
            rowData.setField(3, new GenericMapData(new HashMap<>()));
            data.add(rowData);
        }
        {
            GenericRowData rowData = new GenericRowData(4);
            rowData.setField(0, new BinaryStringData("_col_0_string_4"));
            rowData.setField(1, 4);
            rowData.setField(2, new GenericArrayData(new Object[0]));
            Map<StringData, RowData> mapDataMap = new HashMap<>();
            mapDataMap.put(null, null);
            rowData.setField(3, new GenericMapData(mapDataMap));
            data.add(rowData);
        }
        return data;
    }

    private String initNestedTypesFile(List<RowData> data) throws Exception {
        LogicalType[] fieldTypes = new LogicalType[4];
        fieldTypes[0] = new VarCharType();
        fieldTypes[1] = new IntType();
        List<RowType.RowField> arrayRowFieldList =
                Collections.singletonList(new RowType.RowField("_col2_col0", new VarCharType()));
        fieldTypes[2] = new ArrayType(new RowType(arrayRowFieldList));
        List<RowType.RowField> mapRowFieldList =
                Arrays.asList(
                        new RowType.RowField("_col3_col0", new VarCharType()),
                        new RowType.RowField("_col3_col1", new TimestampType()));
        fieldTypes[3] = new MapType(new VarCharType(), new RowType(mapRowFieldList));
        String schema =
                "struct<_col0:string,_col1:int,_col2:array<struct<_col2_col0:string>>,"
                        + "_col3:map<string,struct<_col3_col0:string,_col3_col1:timestamp>>>";

        File outDir = TEMPORARY_FOLDER.newFolder();
        Properties writerProps = new Properties();
        writerProps.setProperty("orc.compress", "LZ4");

        final OrcBulkWriterFactory<RowData> writer =
                new OrcBulkWriterFactory<>(
                        new RowDataVectorizer(schema, fieldTypes),
                        writerProps,
                        new Configuration());

        StreamingFileSink<RowData> sink =
                StreamingFileSink.forBulkFormat(
                                new org.apache.flink.core.fs.Path(outDir.toURI()), writer)
                        .withBucketCheckInterval(10000)
                        .build();

        try (OneInputStreamOperatorTestHarness<RowData, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 1, 1, 0)) {

            testHarness.setup();
            testHarness.open();

            int time = 0;
            for (final RowData record : data) {
                testHarness.processElement(record, ++time);
            }
            testHarness.snapshot(1, ++time);
            testHarness.notifyOfCompletedCheckpoint(1);
        }
        return outDir.getAbsolutePath();
    }

    @Test
    public void testLimitableBulkFormat() throws ExecutionException, InterruptedException {
        super.tableEnv()
                .executeSql(
                        "insert into orcLimitTable select x, y, " + "1 as a " + "from originalT")
                .await();
        TableResult tableResult1 =
                super.tableEnv().executeSql("SELECT * FROM orcLimitTable limit 5");
        List<Row> rows1 = CollectionUtil.iteratorToList(tableResult1.collect());
        assertEquals(5, rows1.size());

        check(
                "select a from orcLimitTable limit 5",
                Arrays.asList(Row.of(1), Row.of(1), Row.of(1), Row.of(1), Row.of(1)));
    }
}
