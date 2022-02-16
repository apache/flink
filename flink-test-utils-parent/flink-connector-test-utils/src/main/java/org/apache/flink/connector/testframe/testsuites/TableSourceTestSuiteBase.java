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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.connector.testframe.external.source.TableSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.connector.testframe.utils.CollectIteratorAssertions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Base class for table source test suites. */
public abstract class TableSourceTestSuiteBase extends AbstractTableTestSuiteBase {
    private static final Logger LOG = LoggerFactory.getLogger(TableSourceTestSuiteBase.class);

    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    /**
     * Test data types for connector table source.
     *
     * <p>This test will insert records, and read back via a Flink job from the source table.
     */
    @TestTemplate
    @DisplayName("Test table source basic read")
    public void testBasicRead(
            TestEnvironment testEnv,
            TableSourceExternalContext externalContext,
            CheckpointingMode semantic)
            throws Exception {
        testTableTypes(testEnv, externalContext, semantic, Arrays.asList(DataTypes.STRING()));
    }

    /**
     * Test data types for connector table source.
     *
     * <p>This test will insert records, and read back via a Flink job from the source table.
     *
     * <p>Now only test basic types.
     */
    @TestTemplate
    @DisplayName("Test table source data type")
    public void testTableDataType(
            TestEnvironment testEnv,
            TableSourceExternalContext externalContext,
            CheckpointingMode semantic)
            throws Exception {
        testTableTypes(testEnv, externalContext, semantic, supportTypes());
    }

    private void testTableTypes(
            TestEnvironment testEnv,
            TableSourceExternalContext externalContext,
            CheckpointingMode semantic,
            List<DataType> supportTypes)
            throws Exception {
        TestingSourceSettings sourceSettings =
                getTestingSourceOptions(Boundedness.CONTINUOUS_UNBOUNDED, semantic);
        StreamExecutionEnvironment env =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Map<String, String> tableOptions = getTableOptions(externalContext, sourceSettings);
        String tableName = "TableSourceTest" + semantic.toString().replaceAll("-", "");
        String createTableSql = getCreateTableSql(tableName, tableOptions, supportTypes);
        tEnv.executeSql(createTableSql);

        List<List<RowData>> testRecordCollections = new LinkedList<>();
        final DataType tableSchema = getTableSchema(supportTypes);
        int splitNum = 4;
        for (int i = 0; i < splitNum; i++) {
            testRecordCollections.add(
                    generateAndWriteTestData(
                            i, externalContext, sourceSettings, tableSchema, supportTypes));
        }

        DataStream<Row> result =
                tEnv.toDataStream(tEnv.sqlQuery(getSelectSql(tableName)), tableSchema);
        try (final CloseableIterator<RowData> resultIterator =
                new RowDataConverterIterator(
                        result.executeAndCollect("Table type test"), tableSchema)) {
            checkResultWithSemantic(
                    resultIterator,
                    testRecordCollections,
                    semantic,
                    getTestDataSize(testRecordCollections));
        }
    }

    /**
     * Get the size of test data.
     *
     * @param collections test data
     * @return the size of test data
     */
    protected int getTestDataSize(List<List<RowData>> collections) {
        int sumSize = 0;
        for (Collection<RowData> collection : collections) {
            sumSize += collection.size();
        }
        return sumSize;
    }

    /**
     * Compare the test data with the result.
     *
     * <p>If the source is bounded, limit should be null.
     *
     * @param resultIterator the data read from the job
     * @param testData the test data
     * @param semantic the supported semantic, see {@link CheckpointingMode}
     * @param limit expected number of the data to read from the job
     */
    private void checkResultWithSemantic(
            CloseableIterator<RowData> resultIterator,
            List<List<RowData>> testData,
            CheckpointingMode semantic,
            Integer limit) {
        if (limit != null) {
            assertThat(
                            CompletableFuture.supplyAsync(
                                    () -> {
                                        CollectIteratorAssertions.assertThat(resultIterator)
                                                .withNumRecordsLimit(limit)
                                                .matchesRecordsFromSource(testData, semantic);
                                        return true;
                                    }))
                    .succeedsWithin(DEFAULT_COLLECT_DATA_TIMEOUT);
        } else {
            CollectIteratorAssertions.assertThat(resultIterator)
                    .matchesRecordsFromSource(testData, semantic);
        }
    }

    /**
     * Generate a set of test records and write it to the given split writer.
     *
     * @param externalContext External context
     * @return List of generated test records
     */
    protected List<RowData> generateAndWriteTestData(
            int splitIndex,
            TableSourceExternalContext externalContext,
            TestingSourceSettings sourceSettings,
            DataType physicalDataType,
            List<DataType> supportTypes) {
        final List<RowData> testRecords =
                generateTestData(splitIndex, ThreadLocalRandom.current().nextLong(), supportTypes);
        LOG.debug("Writing {} records to external system", testRecords.size());
        externalContext
                .createSplitRowDataWriter(sourceSettings, physicalDataType)
                .writeRecords(testRecords);
        return testRecords;
    }

    protected List<RowData> generateTestData(
            int splitIndex, long seed, List<DataType> supportTypes) {
        Random random = new Random(seed);
        List<RowData> testRecords = new ArrayList<>();
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        for (int i = 0; i < recordNum; i++) {
            testRecords.add(generateTestRowData(splitIndex, supportTypes));
        }
        return testRecords;
    }

    private TestingSourceSettings getTestingSourceOptions(
            Boundedness boundedness, CheckpointingMode checkpointingMode) {
        return TestingSourceSettings.builder()
                .setCheckpointingMode(checkpointingMode)
                .setBoundedness(boundedness)
                .build();
    }

    private Map<String, String> getTableOptions(
            TableSourceExternalContext externalContext, TestingSourceSettings sourceSettings) {
        try {
            return externalContext.getSourceTableOptions(sourceSettings);
        } catch (UnsupportedOperationException e) {
            // abort the test
            throw new TestAbortedException("Not support this test.", e);
        }
    }

    class RowDataConverterIterator implements CloseableIterator<RowData> {
        private final CloseableIterator<Row> rowIterator;
        private final DataType schema;

        public RowDataConverterIterator(CloseableIterator<Row> rowIterator, DataType schema) {
            this.rowIterator = rowIterator;
            this.schema = schema;
        }

        @Override
        public void close() throws Exception {
            rowIterator.close();
        }

        @Override
        public boolean hasNext() {
            return rowIterator.hasNext();
        }

        @Override
        public RowData next() {
            return convertToRowData(rowIterator.next(), schema);
        }

        @Override
        public void remove() {
            rowIterator.remove();
        }

        @Override
        public void forEachRemaining(Consumer action) {
            rowIterator.forEachRemaining(action);
        }
    }
}
