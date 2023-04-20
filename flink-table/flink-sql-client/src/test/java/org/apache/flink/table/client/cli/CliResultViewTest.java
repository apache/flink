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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.result.ChangelogResult;
import org.apache.flink.table.client.gateway.result.MaterializedResult;
import org.apache.flink.table.client.util.CliClientTestUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.jline.terminal.Terminal;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Contains basic tests for the {@link CliResultView}. */
class CliResultViewTest {

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

        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("Null Field", DataTypes.STRING()));
        final ResultDescriptor descriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);

        try (Terminal terminal = TerminalUtils.createDumbTerminal()) {
            Thread resultViewRunner =
                    new Thread(
                            new TestingCliResultView(
                                    terminal,
                                    descriptor,
                                    isTableMode,
                                    typedResult,
                                    cancellationCounterLatch));
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

    private static final class TestingCliResultView implements Runnable {

        private final CliResultView<?> realResultView;

        @SuppressWarnings("unchecked")
        public TestingCliResultView(
                Terminal terminal,
                ResultDescriptor descriptor,
                boolean isTableMode,
                TypedResult<?> typedResult,
                CountDownLatch cancellationCounterLatch) {

            if (isTableMode) {
                realResultView =
                        new CliTableResultView(
                                terminal,
                                descriptor,
                                new TestMaterializedResult(
                                        (TypedResult<Integer>) typedResult,
                                        cancellationCounterLatch));
            } else {
                realResultView =
                        new CliChangelogResultView(
                                terminal,
                                descriptor,
                                new TestChangelogResult(
                                        (TypedResult<List<RowData>>) typedResult,
                                        cancellationCounterLatch));
            }
        }

        @Override
        public void run() {
            realResultView.open();
        }
    }

    private static class TestMaterializedResult implements MaterializedResult {

        private final CountDownLatch cancellationCounter;
        private final TypedResult<Integer> typedResult;

        public TestMaterializedResult(
                TypedResult<Integer> typedResult, CountDownLatch cancellationCounterLatch) {
            this.typedResult = typedResult;
            this.cancellationCounter = cancellationCounterLatch;
        }

        @Override
        public void close() {
            cancellationCounter.countDown();
        }

        @Override
        public TypedResult<Integer> snapshot(int pageSize) {
            return typedResult;
        }

        @Override
        public List<RowData> retrievePage(int page) {
            return Collections.singletonList(new GenericRowData(1));
        }
    }

    private static class TestChangelogResult implements ChangelogResult {

        private final TypedResult<List<RowData>> typedResult;
        private final CountDownLatch cancellationCounter;

        public TestChangelogResult(
                TypedResult<List<RowData>> typedResult, CountDownLatch cancellationCounter) {
            this.typedResult = typedResult;
            this.cancellationCounter = cancellationCounter;
        }

        @Override
        public TypedResult<List<RowData>> retrieveChanges() {
            return typedResult;
        }

        @Override
        public void close() {
            cancellationCounter.countDown();
        }
    }
}
