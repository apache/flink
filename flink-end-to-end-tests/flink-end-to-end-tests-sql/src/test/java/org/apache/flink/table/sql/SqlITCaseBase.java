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

package org.apache.flink.table.sql;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.deserialization.TestingDeserializationContext;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResourceExtension;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Base class for sql ITCase. */
@ExtendWith({ParameterizedTestExtension.class, TestLoggerExtension.class})
abstract class SqlITCaseBase {
    private static final Logger LOG = LoggerFactory.getLogger(SqlITCaseBase.class);

    @Parameters(name = "executionMode")
    private static Collection<String> data() {
        return Arrays.asList("streaming", "batch");
    }

    @RegisterExtension
    private final FlinkResourceExtension flinkExtension =
            new FlinkResourceExtension(
                    new LocalStandaloneFlinkResourceFactory()
                            .create(
                                    FlinkResourceSetup.builder()
                                            .addConfiguration(getConfiguration())
                                            .build()));

    @TempDir private Path tmp;

    @Parameter String executionMode;

    Path result;

    static final Path SQL_TOOL_BOX_JAR = ResourceTestUtils.getResource(".*SqlToolbox.jar");

    private static Configuration getConfiguration() {
        // we have to enable checkpoint to trigger flushing for filesystem sink
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setString("execution.checkpointing.interval", "5s");
        return flinkConfig;
    }

    @BeforeEach
    void before() throws Exception {
        LOG.info("The current temporary path: {}", tmp);
        this.result = tmp.resolve(String.format("result-%s", UUID.randomUUID()));
    }

    void runAndCheckSQL(String sqlPath, List<String> resultItems) throws Exception {
        runAndCheckSQL(sqlPath, Collections.singletonMap(result, resultItems));
    }

    void runAndCheckSQL(
            String sqlPath,
            List<String> resultItems,
            Function<List<String>, List<String>> formatter)
            throws Exception {
        runAndCheckSQL(
                sqlPath,
                Collections.singletonMap(result, resultItems),
                Collections.singletonMap(result, formatter),
                Collections.emptyList());
    }

    void runAndCheckSQL(String sqlPath, Map<Path, List<String>> resultItems) throws Exception {
        runAndCheckSQL(sqlPath, resultItems, Collections.emptyMap(), Collections.emptyList());
    }

    void runAndCheckSQL(
            String sqlPath,
            Map<Path, List<String>> resultItems,
            Map<Path, Function<List<String>, List<String>>> formatters,
            List<URI> dependencies)
            throws Exception {
        try (ClusterController clusterController =
                flinkExtension.getFlinkResource().startCluster(1)) {
            List<String> sqlLines = initializeSqlLines(sqlPath);

            executeSqlStatements(clusterController, sqlLines, dependencies);

            // Wait until all the results flushed to the json file.
            LOG.info("Verify the result.");
            for (Map.Entry<Path, List<String>> entry : resultItems.entrySet()) {
                checkResultFile(
                        entry.getKey(),
                        entry.getValue(),
                        formatters.getOrDefault(entry.getKey(), this::formatRawResult));
            }
            LOG.info("The SQL client test run successfully.");
        }
    }

    Map<String, String> generateReplaceVars() {
        Map<String, String> varsMap = new HashMap<>();
        varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
        varsMap.put("$MODE", this.executionMode);
        return varsMap;
    }

    abstract void executeSqlStatements(
            ClusterController clusterController, List<String> sqlLines, List<URI> dependencies)
            throws Exception;

    private List<String> initializeSqlLines(String sqlPath) throws IOException {
        URL url = SqlITCaseBase.class.getClassLoader().getResource(sqlPath);
        if (url == null) {
            throw new FileNotFoundException(sqlPath);
        }
        Map<String, String> vars = generateReplaceVars();
        List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace(var.getKey(), var.getValue());
            }
            result.add(line);
        }

        return result;
    }

    private static void checkResultFile(
            Path resultPath,
            List<String> expectedItems,
            Function<List<String>, List<String>> resultFormatter)
            throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(20));
        List<String> lines = null;
        while (deadline.hasTimeLeft()) {
            if (Files.exists(resultPath)) {
                lines = readResultFiles(resultPath);
                try {
                    List<String> actual = resultFormatter.apply(lines);
                    assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedItems);
                    success = true;
                    break;
                } catch (AssertionError e) {
                    LOG.info(
                            "The target result {} does not match expected records, current {} records, left time: {}s",
                            resultPath,
                            lines.size(),
                            deadline.timeLeft().getSeconds());
                }
            } else {
                LOG.info("The target result {} does not exist now", resultPath);
            }
            Thread.sleep(500);
        }
        assertThat(success)
                .withFailMessage(
                        "Did not get expected results before timeout, actual result: %s.", lines)
                .isTrue();
    }

    private static List<String> readResultFiles(Path path) throws Exception {
        File filePath = path.toFile();
        // list all the non-hidden files
        File[] files = filePath.listFiles((dir, name) -> !name.startsWith("."));
        List<String> result = new ArrayList<>();
        if (files != null) {
            for (File file : files) {
                result.addAll(Files.readAllLines(file.toPath()));
            }
        }
        return result;
    }

    /**
     * The raw data read from the file system can be mapped and transformed. For example, subclasses
     * can override this method to obtain the final result after materialization.
     *
     * <pre>{@code
     * @Override
     * List<String> formatRawResult(List<String> rawResults) {
     *     return convertToMaterializedResult(rawResults, schema, deserializationSchema);
     * }
     * }</pre>
     */
    List<String> formatRawResult(List<String> rawResults) {
        return rawResults;
    }

    static List<String> convertToMaterializedResult(
            List<String> rawResults,
            ResolvedSchema schema,
            DeserializationSchema<RowData> deserializationSchema) {
        DataCollector collector = new DataCollector();
        try {
            deserializationSchema.open(new TestingDeserializationContext());
            for (String rawResult : rawResults) {
                deserializationSchema.deserialize(rawResult.getBytes(), collector);
            }
        } catch (Exception e) {
            fail("deserialize error: ", e);
        }

        RowRowConverter converter = RowRowConverter.create(schema.toPhysicalRowDataType());
        Map<Row, Row> upsertResult = new HashMap<>();

        for (RowData rowData : collector.dataList) {
            RowKind kind = rowData.getRowKind();

            Row row = converter.toExternal(rowData);
            assertThat(row).isNotNull();

            Row key = Row.project(row, schema.getPrimaryKeyIndexes());
            key.setKind(RowKind.INSERT);

            if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                Row upsertRow = Row.copy(row);
                upsertRow.setKind(RowKind.INSERT);
                upsertResult.put(key, upsertRow);
            } else {
                Row oldValue = upsertResult.remove(key);
                if (oldValue == null) {
                    throw new RuntimeException(
                            "Tried to delete a value that wasn't inserted first. "
                                    + "This is probably an incorrectly implemented test.");
                }
            }
        }

        return upsertResult.values().stream().map(Row::toString).collect(Collectors.toList());
    }

    /**
     * Create a DebeziumJsonDeserializationSchema using the given {@link ResolvedSchema} to convert
     * debezium-json formatted record into {@link RowData}.
     */
    static DebeziumJsonDeserializationSchema createDebeziumDeserializationSchema(
            ResolvedSchema schema) {
        return new DebeziumJsonDeserializationSchema(
                schema.toPhysicalRowDataType(),
                Collections.emptyList(),
                InternalTypeInfo.of(schema.toPhysicalRowDataType().getLogicalType()),
                false,
                true,
                TimestampFormat.ISO_8601);
    }

    private static class DataCollector implements Collector<RowData> {

        private final List<RowData> dataList = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            dataList.add(record);
        }

        @Override
        public void close() {}
    }
}
