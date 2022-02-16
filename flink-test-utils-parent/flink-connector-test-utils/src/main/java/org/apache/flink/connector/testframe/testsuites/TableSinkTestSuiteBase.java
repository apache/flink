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

package org.apache.flink.connector.testframe.testsuites;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.TableSinkExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE;
import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Base class for table sink test suites. */
public abstract class TableSinkTestSuiteBase extends AbstractTableTestSuiteBase {
    private static final Logger LOG = LoggerFactory.getLogger(TableSinkTestSuiteBase.class);

    private static final int NUM_RECORDS_UPPER_BOUND = 200;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    /**
     * Test connector table sink.
     *
     * <p>This test will insert records to the sink table, and read back.
     */
    @TestTemplate
    @DisplayName("Test table sink basic write")
    public void testBaseWrite(
            TestEnvironment testEnv,
            TableSinkExternalContext externalContext,
            CheckpointingMode semantic)
            throws Exception {
        testTableTypes(testEnv, externalContext, semantic, Arrays.asList(DataTypes.STRING()));
    }

    /**
     * Test data types for connector table sink.
     *
     * <p>This test will insert records to the sink table, and read back.
     *
     * <p>Now only test basic types.
     */
    @TestTemplate
    @DisplayName("Test table sink data type")
    public void testTableDataType(
            TestEnvironment testEnv,
            TableSinkExternalContext externalContext,
            CheckpointingMode semantic)
            throws Exception {
        testTableTypes(testEnv, externalContext, semantic, supportTypes());
    }

    private void testTableTypes(
            TestEnvironment testEnv,
            TableSinkExternalContext externalContext,
            CheckpointingMode semantic,
            List<DataType> supportTypes)
            throws Exception {
        TestingSinkSettings sinkOptions = getTestingSinkOptions(semantic);
        StreamExecutionEnvironment env =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Map<String, String> tableOptions = getTableOptions(externalContext, sinkOptions);
        String tableName = "TableSinkTest" + semantic.toString().replaceAll("-", "");
        tEnv.executeSql(getCreateTableSql(tableName, tableOptions, supportTypes));

        Tuple2<String, List<RowData>> tuple2 =
                initialValues(
                        tableName,
                        ThreadLocalRandom.current().nextLong(),
                        NUM_RECORDS_UPPER_BOUND,
                        NUM_RECORDS_LOWER_BOUND,
                        supportTypes);
        String initialValuesSql = tuple2.f0;
        tEnv.executeSql(initialValuesSql).await();

        List<RowData> expectedResult = tuple2.f1;
        assertThat(
                        checkGetEnoughRecordsWithSemantic(
                                expectedResult,
                                pollAndAppendResultData(
                                        new ArrayList<>(),
                                        externalContext.createSinkRowDataReader(
                                                sinkOptions, getTableSchema(supportTypes)),
                                        expectedResult,
                                        30,
                                        semantic),
                                semantic))
                .isTrue();
    }

    private TestingSinkSettings getTestingSinkOptions(CheckpointingMode checkpointingMode) {
        return TestingSinkSettings.builder().setCheckpointingMode(checkpointingMode).build();
    }

    private Map<String, String> getTableOptions(
            TableSinkExternalContext externalContext, TestingSinkSettings sinkSettings) {
        try {
            return externalContext.getSinkTableOptions(sinkSettings);
        } catch (UnsupportedOperationException e) {
            // abort the test
            throw new TestAbortedException("Not support this test.", e);
        }
    }

    /**
     * Poll records from the sink.
     *
     * @param result Append records to which list
     * @param reader The sink reader
     * @param expected The expected list which help to stop polling
     * @param retryTimes The retry times
     * @param semantic The semantic
     * @return Collection of records in the Sink
     */
    private List<RowData> pollAndAppendResultData(
            List<RowData> result,
            ExternalSystemDataReader<RowData> reader,
            List<RowData> expected,
            int retryTimes,
            CheckpointingMode semantic) {
        long timeoutMs = 1000L;
        int retryIndex = 0;

        while (retryIndex++ < retryTimes
                && !checkGetEnoughRecordsWithSemantic(expected, result, semantic)) {
            result.addAll(reader.poll(Duration.ofMillis(timeoutMs)));
        }
        return result;
    }

    /**
     * Check whether the polling should stop.
     *
     * @param expected The expected list which help to stop polling
     * @param result The records that have been read
     * @param semantic The semantic
     * @return Whether the polling should stop
     */
    private boolean checkGetEnoughRecordsWithSemantic(
            List<RowData> expected, List<RowData> result, CheckpointingMode semantic) {
        checkNotNull(expected);
        checkNotNull(result);
        if (EXACTLY_ONCE.equals(semantic)) {
            return expected.size() <= result.size();
        } else if (AT_LEAST_ONCE.equals(semantic)) {
            Set<Integer> matchedIndex = new HashSet<>();
            for (RowData record : expected) {
                int before = matchedIndex.size();
                for (int i = 0; i < result.size(); i++) {
                    if (matchedIndex.contains(i)) {
                        continue;
                    }
                    if (record.equals(result.get(i))) {
                        matchedIndex.add(i);
                        break;
                    }
                }
                // if not find the record in the result
                if (before == matchedIndex.size()) {
                    return false;
                }
            }
            return true;
        }
        throw new IllegalStateException(
                String.format("%s delivery guarantee doesn't support test.", semantic.name()));
    }
}
