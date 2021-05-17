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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.types.Row;

import org.jline.reader.MaskingCallback;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/** Contains basic tests for the {@link CliResultView}. */
public class CliResultViewTest {

    @Test
    public void testTableResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(TypedResult.endOfStream(), true, 0);
    }

    @Test
    public void testTableResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(TypedResult.empty(), true, 1);
    }

    @Test
    public void testTableResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(TypedResult.payload(1), true, 1);
    }

    @Test
    public void testChangelogResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(TypedResult.endOfStream(), false, 0);
    }

    @Test
    public void testChangelogResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(TypedResult.empty(), false, 1);
    }

    @Test
    public void testChangelogResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(TypedResult.payload(Collections.emptyList()), false, 1);
    }

    private void testResultViewClearResult(
            TypedResult<?> typedResult, boolean isTableMode, int expectedCancellationCount)
            throws Exception {
        final CountDownLatch cancellationCounterLatch =
                new CountDownLatch(expectedCancellationCount);

        final MockExecutor executor = new MockExecutor(typedResult, cancellationCounterLatch);
        String sessionId = executor.openSession("test-session");
        final ResultDescriptor descriptor =
                new ResultDescriptor(
                        "result-id",
                        ResolvedSchema.of(Column.physical("Null Field", DataTypes.STRING())),
                        false,
                        false,
                        true);

        try (CliClient cli =
                new TestingCliClient(
                        TerminalUtils.createDumbTerminal(),
                        sessionId,
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

        assertTrue(
                "Invalid number of cancellations.",
                cancellationCounterLatch.await(10, TimeUnit.SECONDS));
    }

    private static final class MockExecutor implements Executor {

        private final TypedResult<?> typedResult;
        private final CountDownLatch cancellationCounter;
        private static final Configuration defaultConfig =
                TableConfig.getDefault().getConfiguration();

        public MockExecutor(TypedResult<?> typedResult, CountDownLatch cancellationCounter) {
            this.typedResult = typedResult;
            this.cancellationCounter = cancellationCounter;
        }

        @Override
        public void start() throws SqlExecutionException {
            // do nothing
        }

        @Override
        public String openSession(@Nullable String sessionId) throws SqlExecutionException {
            return sessionId;
        }

        @Override
        public void closeSession(String sessionId) throws SqlExecutionException {
            // do nothing
        }

        @Override
        public Map<String, String> getSessionConfigMap(String sessionId)
                throws SqlExecutionException {
            return defaultConfig.toMap();
        }

        @Override
        public ReadableConfig getSessionConfig(String sessionId) throws SqlExecutionException {
            return defaultConfig;
        }

        @Override
        public void resetSessionProperties(String sessionId) throws SqlExecutionException {}

        @Override
        public void resetSessionProperty(String sessionId, String key)
                throws SqlExecutionException {}

        @Override
        public void setSessionProperty(String sessionId, String key, String value)
                throws SqlExecutionException {}

        @Override
        public Operation parseStatement(String sessionId, String statement)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public List<String> completeStatement(String sessionId, String statement, int position) {
            return null;
        }

        @Override
        public TableResult executeOperation(String sessionId, Operation operation)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public TableResult executeModifyOperations(
                String sessionId, List<ModifyOperation> operations) throws SqlExecutionException {
            return null;
        }

        @Override
        public ResultDescriptor executeQuery(String sessionId, QueryOperation query)
                throws SqlExecutionException {
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public TypedResult<List<Row>> retrieveResultChanges(String sessionId, String resultId)
                throws SqlExecutionException {
            return (TypedResult<List<Row>>) typedResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public TypedResult<Integer> snapshotResult(String sessionId, String resultId, int pageSize)
                throws SqlExecutionException {
            return (TypedResult<Integer>) typedResult;
        }

        @Override
        public List<Row> retrieveResultPage(String resultId, int page)
                throws SqlExecutionException {
            return Collections.singletonList(new Row(1));
        }

        @Override
        public void cancelQuery(String sessionId, String resultId) throws SqlExecutionException {
            cancellationCounter.countDown();
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
                String sessionId,
                Executor executor,
                Path historyFilePath,
                @Nullable MaskingCallback inputTransformer) {
            super(() -> terminal, sessionId, executor, historyFilePath, inputTransformer);
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
