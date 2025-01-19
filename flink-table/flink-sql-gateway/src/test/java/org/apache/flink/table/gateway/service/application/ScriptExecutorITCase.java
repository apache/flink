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

package org.apache.flink.table.gateway.service.application;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.table.gateway.AbstractSqlGatewayStatementITCase;
import org.apache.flink.table.gateway.AbstractSqlGatewayStatementITCaseBase;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.utils.TestSqlStatement;
import org.apache.flink.test.util.MiniClusterPipelineExecutorServiceLoader;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase to run the script with {@link ScriptExecutor}. */
@ExtendWith(ParameterizedTestExtension.class)
public class ScriptExecutorITCase extends AbstractSqlGatewayStatementITCaseBase {

    private ScriptExecutor executor;

    @Override
    protected String runStatements(List<TestSqlStatement> statements) throws Exception {
        // Prepare the cluster
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        configuration.set(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, false);
        configuration.set(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);

        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();

        // Execute
        try (MiniCluster miniCluster = new MiniCluster(clusterConfiguration);
                OutputStream outputStream = new ByteArrayOutputStream(1024)) {
            miniCluster.start();
            MiniClusterPipelineExecutorServiceLoader loader =
                    new MiniClusterPipelineExecutorServiceLoader(miniCluster);
            StreamContextEnvironment.setAsContext(
                    loader,
                    miniCluster.getConfiguration(),
                    ScriptExecutor.class.getClassLoader(),
                    false,
                    false);

            executor =
                    new TestScriptExecutor(
                            SessionContext.create(
                                    new DefaultContext(
                                            miniCluster.getConfiguration(),
                                            Collections.emptyList()),
                                    SessionHandle.create(),
                                    SessionEnvironment.newBuilder()
                                            .setSessionEndpointVersion(
                                                    SqlGatewayRestAPIVersion.getDefaultVersion())
                                            .build(),
                                    Executors.newDirectExecutorService()),
                            new TestPrinter(statements.iterator(), outputStream));
            executor.execute(
                    String.join(
                            "\n",
                            statements.stream()
                                    .map(TestSqlStatement::getSql)
                                    .collect(Collectors.joining())));
            return outputStream.toString();
        }
    }

    @Test
    void testParseStatementWithComments() throws Exception {
        assertThat(runScript("comment.q"))
                .contains(
                        "+------+------+------+-----+--------+-----------+"
                                + System.lineSeparator()
                                + "| name | type | null | key | extras | watermark |"
                                + System.lineSeparator()
                                + "+------+------+------+-----+--------+-----------+"
                                + System.lineSeparator()
                                + "|    a |  INT | TRUE |     |        |           |"
                                + System.lineSeparator()
                                + "|    b |  INT | TRUE |     |        |           |"
                                + System.lineSeparator()
                                + "+------+------+------+-----+--------+-----------+");
    }

    @Test
    void testParseStatementWithoutSemicolon() throws Exception {
        assertThat(runScript("no_semicolon.q"))
                .contains(
                        "+------+------+------+-----+--------+-----------+"
                                + System.lineSeparator()
                                + "| name | type | null | key | extras | watermark |"
                                + System.lineSeparator()
                                + "+------+------+------+-----+--------+-----------+"
                                + System.lineSeparator()
                                + "|    a |  INT | TRUE |     |        |           |"
                                + System.lineSeparator()
                                + "|    b |  INT | TRUE |     |        |           |"
                                + System.lineSeparator()
                                + "+------+------+------+-----+--------+-----------+");
    }

    @Test
    void testParseErrorPositionIsCorrect() throws Exception {
        assertThat(runScript("error.q"))
                .contains(
                        "org.apache.flink.table.api.SqlParserException: SQL parse failed. Encountered \")\" at line 26, column 1.");
    }

    @Override
    protected boolean isStreaming() {
        return executor.context.getSessionConf().get(RUNTIME_MODE)
                == RuntimeExecutionMode.STREAMING;
    }

    @Override
    protected boolean skip() {
        return parameters.getSqlPath().contains("set.q");
    }

    private static class TestScriptExecutor extends ScriptExecutor {

        public TestScriptExecutor(SessionContext context, Printer printer) {
            super(context, printer);
        }

        @Override
        public void execute(String script) {
            ResultIterator iterator = new ResultIterator(script);
            while (true) {
                try {
                    if (iterator.hasNext()) {
                        Result result = iterator.next();
                        printer.print(result.statement);
                        if (result.error != null) {
                            throw result.error;
                        } else {
                            printer.print(checkNotNull(result.fetcher));
                        }
                    } else {
                        break;
                    }
                } catch (Throwable t) {
                    printer.print(t);
                }
            }
        }
    }

    private class TestPrinter extends Printer {

        private final Iterator<TestSqlStatement> statements;
        private String statement;

        public TestPrinter(Iterator<TestSqlStatement> statements, OutputStream stream) {
            super(stream);
            this.statements = statements;
        }

        @Override
        public void print(String statement) {
            if (statements.hasNext()) {
                writer.print(statements.next().getComment());
            }
            this.statement = statement;
            writer.print(statement);
            writer.flush();
        }

        @Override
        public void print(ResultFetcher fetcher) {
            StatementType statementType = StatementType.match(statement);
            try {
                writer.print(
                        ScriptExecutorITCase.this.toString(
                                statementType,
                                fetcher.getResultSchema(),
                                fetcher.getConverter(),
                                new RowDataIterator(fetcher)));
                writer.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void print(Throwable t) {
            Throwable root = t;
            while (root.getCause() != null
                    && root.getCause().getMessage() != null
                    && !root.getCause().getMessage().isEmpty()) {
                root = root.getCause();
            }
            writer.print(
                    Tag.ERROR.addTag(
                            root.getClass().getName()
                                    + ": "
                                    + removeRowNumber(root.getMessage().trim())
                                    + "\n"));
            writer.flush();
        }
    }

    private String runScript(String fileName) throws Exception {
        try (OutputStream outputStream = new ByteArrayOutputStream(1024)) {
            executor =
                    new ScriptExecutor(
                            SessionContext.create(
                                    new DefaultContext(
                                            new Configuration(), Collections.emptyList()),
                                    SessionHandle.create(),
                                    SessionEnvironment.newBuilder()
                                            .setSessionEndpointVersion(
                                                    SqlGatewayRestAPIVersion.getDefaultVersion())
                                            .build(),
                                    Executors.newDirectExecutorService()),
                            new Printer(outputStream));
            String input =
                    IOUtils.toString(
                            checkNotNull(
                                    AbstractSqlGatewayStatementITCase.class.getResourceAsStream(
                                            "/application/" + fileName)),
                            StandardCharsets.UTF_8);
            try {
                executor.execute(input);
            } catch (Exception e) {
                // ignore
            }
            return outputStream.toString();
        }
    }
}
