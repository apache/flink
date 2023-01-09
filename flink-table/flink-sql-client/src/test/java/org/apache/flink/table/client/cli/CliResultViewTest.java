/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.utils.DateTimeUtils;

import org.jline.reader.MaskingCallback;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Contains basic tests for the {@link CliResultView}. */
class CliResultViewTest {

    private static final String SESSION_ID = "test-session";

    @Test
    void testTableResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(TypedResult.endOfStream(), true, 0);
    }

    @Test
    void testTableResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(TypedResult.empty(), true, 1);
    }

    @Test
    void testTableResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(TypedResult.payload(1), true, 1);
    }

    @Test
    void testChangelogResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(TypedResult.endOfStream(), false, 0);
    }

    @Test
    void testChangelogResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(TypedResult.empty(), false, 1);
    }

    @Test
    void testChangelogResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(TypedResult.payload(Collections.emptyList()), false, 1);
    }

    private void testResultViewClearResult(
            TypedResult<?> typedResult, boolean isTableMode, int expectedCancellationCount)
            throws Exception {
        final CountDownLatch cancellationCounterLatch =
                new CountDownLatch(expectedCancellationCount);

        final MockExecutor executor = new MockExecutor(typedResult, cancellationCounterLatch);
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        executor.openSession(SESSION_ID);
        ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("Null Field", DataTypes.STRING()));
        final ResultDescriptor descriptor =
                new ResultDescriptor(
                        "result-id",
                        schema,
                        false,
                        testConfig,
                        new RowDataToStringConverterImpl(
                                schema.toPhysicalRowDataType(),
                                DateTimeUtils.UTC_ZONE.toZoneId(),
                                Thread.currentThread().getContextClassLoader(),
                                false));

        try (CliClient cli =
                new TestingCliClient(
                        TerminalUtils.createDumbTerminal(),
                        executor,
                        File.createTempFile("history", "tmp").toPath(),
                        null)) {
            Thread resultViewRunner =
                    new Thread(new TestingCliResultView(cli, descriptor, isTableMode));
            resultViewRunner.start();

            if (!resultViewRunner.isInterrupted()) {
                resultViewRunner.interrupt();
            }
            // close the client until view exit
            while (resultViewRunner.isAlive()) {
                Thread.sleep(100);
            }
        }

        assertThat(cancellationCounterLatch.await(10, TimeUnit.SECONDS))
                .as("Invalid number of cancellations.")
                .isTrue();
    }

    private static final class MockExecutor implements Executor {

        private final TypedResult<?> typedResult;
        private final CountDownLatch cancellationCounter;
        private static final Configuration defaultConfig = new Configuration();

        public MockExecutor(TypedResult<?> typedResult, CountDownLatch cancellationCounter) {
            this.typedResult = typedResult;
            this.cancellationCounter = cancellationCounter;
        }

        @Override
        public void start() throws SqlExecutionException {
            // do nothing
        }

        @Override
        public void openSession(@Nullable String sessionId) throws SqlExecutionException {
            // do nothing
        }

        @Override
        public void closeSession() throws SqlExecutionException {
            // do nothing
        }

        @Override
        public Map<String, String> getSessionConfigMap() throws SqlExecutionException {
            return defaultConfig.toMap();
        }

        @Override
        public ReadableConfig getSessionConfig() throws SqlExecutionException {
            return defaultConfig;
        }

        @Override
        public void resetSessionProperties() throws SqlExecutionException {}

        @Override
        public void resetSessionProperty(String key) throws SqlExecutionException {}

        @Override
        public void setSessionProperty(String key, String value) throws SqlExecutionException {}

        @Override
        public Operation parseStatement(String statement) throws SqlExecutionException {
            return null;
        }

        @Override
        public List<String> completeStatement(String statement, int position) {
            return null;
        }

        @Override
        public TableResultInternal executeOperation(Operation operation)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public TableResultInternal executeModifyOperations(List<ModifyOperation> operations)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public ResultDescriptor executeQuery(QueryOperation query) throws SqlExecutionException {
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public TypedResult<List<RowData>> retrieveResultChanges(String resultId)
                throws SqlExecutionException {
            return (TypedResult<List<RowData>>) typedResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public TypedResult<Integer> snapshotResult(String resultId, int pageSize)
                throws SqlExecutionException {
            return (TypedResult<Integer>) typedResult;
        }

        @Override
        public List<RowData> retrieveResultPage(String resultId, int page)
                throws SqlExecutionException {
            return Collections.singletonList(new GenericRowData(1));
        }

        @Override
        public void cancelQuery(String resultId) throws SqlExecutionException {
            cancellationCounter.countDown();
        }

        @Override
        public void removeJar(String jarUrl) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public Optional<String> stopJob(String jobId, boolean isWithSavepoint, boolean isWithDrain)
                throws SqlExecutionException {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    private static final class TestingCliResultView implements Runnable {

        private final CliResultView realResultView;

        public TestingCliResultView(
                CliClient client, ResultDescriptor descriptor, boolean isTableMode) {

            if (isTableMode) {
                realResultView = new TestingCliTableResultView(client, descriptor);
            } else {
                realResultView = new TestingCliChangelogResultView(client, descriptor);
            }
        }

        @Override
        public void run() {
            realResultView.open();
        }
    }

    private static class TestingCliChangelogResultView extends CliChangelogResultView {

        public TestingCliChangelogResultView(CliClient client, ResultDescriptor resultDescriptor) {
            super(client, resultDescriptor);
        }

        @Override
        protected List<AttributedString> computeMainHeaderLines() {
            return Collections.emptyList();
        }
    }

    private static class TestingCliTableResultView extends CliTableResultView {

        public TestingCliTableResultView(CliClient client, ResultDescriptor resultDescriptor) {
            super(client, resultDescriptor);
        }

        @Override
        protected List<AttributedString> computeMainHeaderLines() {
            return Collections.emptyList();
        }
    }

    private static class TestingCliClient extends CliClient {

        private final Terminal terminal;

        public TestingCliClient(
                Terminal terminal,
                Executor executor,
                Path historyFilePath,
                @Nullable MaskingCallback inputTransformer) {
            super(() -> terminal, executor, historyFilePath, inputTransformer);
            this.terminal = terminal;
        }

        @Override
        public Terminal getTerminal() {
            return terminal;
        }

        @Override
        public boolean isPlainTerminal() {
            return true;
        }

        @Override
        public void clearTerminal() {
            // do nothing
        }
    }
}
