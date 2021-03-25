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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.cli.utils.TerminalUtils;
import org.apache.flink.table.client.cli.utils.TestTableResult;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.jline.utils.AttributedString;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/** Contains basic tests for the {@link CliResultView}. */
public class CliResultViewTest {

    @Test
    public void testTableResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(true, 0);
    }

    @Test
    public void testTableResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(true, 1);
    }

    @Test
    public void testTableResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(true, 1);
    }

    @Test
    public void testChangelogResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(false, 0);
    }

    @Test
    public void testChangelogResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(false, 1);
    }

    @Test
    public void testChangelogResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(false, 1);
    }

    private void testResultViewClearResult(boolean isTableMode, int expectedCancellationCount)
            throws Exception {
        final CountDownLatch cancellationCounterLatch =
                new CountDownLatch(expectedCancellationCount);

        final TestingExecutor executor = new TestingExecutor();
        String sessionId = executor.openSession("test-session");
        Configuration configuration = new Configuration();
        if (isTableMode) {
            configuration.set(SqlClientOptions.EXECUTION_RESULT_MODE, ResultMode.TABLE);
        } else {
            configuration.set(SqlClientOptions.EXECUTION_RESULT_MODE, ResultMode.CHANGELOG);
        }
        final ResultDescriptor descriptor =
                ResultDescriptor.createResultDescriptor(
                        configuration,
                        new TestTableResult(
                                ResultKind.SUCCESS_WITH_CONTENT,
                                ResolvedSchema.of(
                                        Column.physical("Null Field", DataTypes.STRING())),
                                CloseableIterator.adapterForIterator(
                                        new MockIterator(), cancellationCounterLatch::countDown)));

        Thread resultViewRunner = null;
        CliClient cli = null;
        try {
            cli =
                    new CliClient(
                            TerminalUtils.createDummyTerminal(),
                            sessionId,
                            executor,
                            File.createTempFile("history", "tmp").toPath(),
                            null);
            resultViewRunner = new Thread(new TestingCliResultView(cli, descriptor, isTableMode));
            resultViewRunner.start();
        } finally {
            if (resultViewRunner != null && !resultViewRunner.isInterrupted()) {
                resultViewRunner.interrupt();
            }
            if (cli != null) {
                cli.close();
            }
        }

        assertTrue(
                "Invalid number of cancellations.",
                cancellationCounterLatch.await(10, TimeUnit.SECONDS));
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

    private static class MockIterator implements Iterator<Row> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Row next() {
            return Row.of(1);
        }
    }
}
