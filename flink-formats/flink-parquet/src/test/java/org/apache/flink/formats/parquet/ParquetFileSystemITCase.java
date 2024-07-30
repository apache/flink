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

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.FILTER_PREDICATE;
import static org.apache.parquet.hadoop.ParquetInputFormat.setFilterPredicate;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link ParquetFileFormatFactory}. */
@ExtendWith(ParameterizedTestExtension.class)
public class ParquetFileSystemITCase extends BatchFileSystemITCaseBase {

    @Parameter public boolean configure;

    @Parameters(name = "configure={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Override
    @BeforeEach
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
    @TestTemplate
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
                assertThat(footer.getBlocks().get(0).getColumns().get(0).getCodec())
                        .hasToString("GZIP");
            } else {
                assertThat(footer.getBlocks().get(0).getColumns().get(0).getCodec())
                        .hasToString("SNAPPY");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    void testLimitableBulkFormat() throws ExecutionException, InterruptedException {
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

    @TestTemplate
    void testParquetFilterPredicate() throws IOException, ExecutionException, InterruptedException {
        // create a temporary parquet file with number of rows slightly greater than read batch size
        String path = TempDirUtils.newFolder(super.fileTempFolder()).toURI().getPath();
        int nRows = VectorizedColumnBatch.DEFAULT_SIZE + 10;
        createParquetFile(nRows, path);

        // search one record in the parquet file, the is in the second read batch
        int x = nRows - 2;
        // set parquet file level filter
        FilterPredicate predicate = eq(intColumn("x"), x);
        Configuration config = new Configuration();
        setFilterPredicate(config, predicate);
        String filterBase64 = config.get(FILTER_PREDICATE);
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table data("
                                        + "x int"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'path' = '%s',"
                                        + "'format' = 'parquet',"
                                        + "'%s' = '%s'"
                                        + ")",
                                path, FILTER_PREDICATE, filterBase64));
        // assert that we found exactly one row of value x
        check(
                String.format("select * from data where x = %s", x),
                Collections.singletonList(Row.of(x)));
    }

    private void createParquetFile(int nRows, String path)
            throws InterruptedException, ExecutionException {
        List<Row> rows = IntStream.range(0, nRows).boxed().map(Row::of).collect(toList());
        tableEnv().createTemporaryView("t_in", tableEnv().fromValues(rows).as("x"));
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table t_out("
                                        + "x int"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'path' = '%s',"
                                        + "'format' = 'parquet',"
                                        + "'parquet.page.size' = '100'"
                                        + ")",
                                path));
        super.tableEnv().executeSql("insert into t_out select * from t_in").await();
    }
}
