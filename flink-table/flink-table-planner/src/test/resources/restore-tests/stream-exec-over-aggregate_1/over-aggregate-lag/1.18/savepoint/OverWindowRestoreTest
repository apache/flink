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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.commons.collections.CollectionUtils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.source.enumerator.NoOpEnumState;
import org.apache.flink.connector.source.enumerator.NoOpEnumStateSerializer;
import org.apache.flink.connector.source.enumerator.ValuesSourceEnumerator;
import org.apache.flink.connector.source.split.ValuesSourceSplit;
import org.apache.flink.connector.source.split.ValuesSourceSplitSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

// Run this test to generate a savepoint in Flink 1.18.
// In order to run the test, you need to cherry-pick commits:
// - dcce3764a4500b2006cd260677169d14c553a3eb
// - e914eb7fc3f31286ed7e33cc93e7ffbca785b731
// - a5b4e60e563bf145afede43dda8510833d2932e4
@ExtendWith(MiniClusterExtension.class)
class OverWindowRestoreTest {

    private @TempDir Path tmpDir;

    private final Path savepointDirPath = Paths.get(String.format(
            "%s/src/test/resources/restore-tests/%s_%d/%s/savepoint/",
            System.getProperty("user.dir"), "stream-exec-over-aggregate", 1,
            "lag-function"));

    @Test
    void testOverWindowRestore() throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        settings.getConfiguration().set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanCompilation.SCHEMA);

            final String id = TestValuesTableFactory.registerData(
                    Collections.singletonList(
                            Row.of("2020-04-15 08:00:05", Collections.singletonMap(42.0, 42.0))
                    )
            );

            tEnv.executeSql(
                    "CREATE TABLE t(\n"
                            + "ts STRING,\n"
                            + "b MAP<DOUBLE, DOUBLE>,\n"
                            + "r_time AS TO_TIMESTAMP(`ts`),\n"
                            + "WATERMARK for `r_time` AS `r_time`\n"
                            + ") WITH(\n"
                            + "'connector' = 'values',\n"
                            + String.format("'data-id' = '%s',\n", id)
                            + "'terminating' = 'false',\n"
                            + "'runtime-source' = 'NewSource'\n"
                            + ")\n"
            );

        CompletableFuture<?> future = new CompletableFuture<>();
        TestValuesTableFactory.registerLocalRawResultsObserver(
            "sink_t",
                (integer, strings) -> {
                    List<String> results = Collections.singletonList("+I[2020-04-15 08:00:05,"
                            + " null]");
                    List<String> currentResults = TestValuesTableFactory.getRawResultsAsStrings("sink_t");
                    final boolean shouldComplete =
                            CollectionUtils.isEqualCollection(currentResults, results);
                    if (shouldComplete) {
                        future.complete(null);
                    }
                }
        );

        tEnv.executeSql(
                "CREATE TABLE sink_t(\n"
                        + "ts STRING,\n"
                        + "b MAP<DOUBLE, DOUBLE>\n"
                        + ") WITH(\n"
                        + "'connector' = 'values',\n"
                        + "'sink-insert-only' = 'false'\n"
                        + ")\n"
        );

        final CompiledPlan compiledPlan = tEnv.compilePlanSql(
                "INSERT INTO sink_t SELECT ts, LAG(b, 1) over (order by r_time) AS bLag FROM t");

        final TableResult tableResult = compiledPlan.execute();
        future.get();
        final JobClient jobClient = tableResult.getJobClient().get();
        final String savepoint =
                jobClient
                        .stopWithSavepoint(false, tmpDir.toString(), SavepointFormatType.DEFAULT)
                        .get();
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
        final Path savepointPath = Paths.get(new URI(savepoint));
        Files.createDirectories(savepointDirPath);
        Files.move(savepointPath, savepointDirPath, StandardCopyOption.ATOMIC_MOVE);
    }
}
