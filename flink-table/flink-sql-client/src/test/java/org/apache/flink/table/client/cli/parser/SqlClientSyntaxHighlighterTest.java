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

package org.apache.flink.table.client.cli.parser;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.util.CloseableIterator;

import org.jline.reader.Parser;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.client.cli.parser.SyntaxHighlightStyle.BuiltInStyle.DARK;
import static org.apache.flink.table.client.cli.parser.SyntaxHighlightStyle.BuiltInStyle.LIGHT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlClientSyntaxHighlighter}. */
class SqlClientSyntaxHighlighterTest {

    private static final String SQL = "select * from t";
    private static final String SET_COLOR_SCHEMA =
            "SET 'sql-client.display.color-schema' = 'light';";

    @Test
    void refreshesConfigurationAfterAcceptedStatement() {
        TestingExecutor executor = new TestingExecutor();
        executor.setColorSchema(DARK);

        SqlClientSyntaxHighlighter highlighter = new SqlClientSyntaxHighlighter(executor);
        SqlMultiLineParser parser =
                new SqlMultiLineParser(
                        new SqlCommandParserImpl(),
                        executor,
                        CliClient.ExecutionMode.INTERACTIVE_EXECUTION,
                        highlighter::updateSessionConfig);

        assertHighlightedAs(highlighter, DARK);
        assertThat(executor.getSessionConfigCalls).isEqualTo(1);

        executor.setColorSchemaAfterExecute(LIGHT);

        assertHighlightedAs(highlighter, DARK);
        assertThat(executor.getSessionConfigCalls).isEqualTo(1);

        parser.parse(SET_COLOR_SCHEMA, SET_COLOR_SCHEMA.length(), Parser.ParseContext.ACCEPT_LINE);

        assertThat(executor.executedStatement).isEqualTo(SET_COLOR_SCHEMA);
        assertHighlightedAs(highlighter, LIGHT);
        assertThat(executor.getSessionConfigCalls).isEqualTo(3);
    }

    @Test
    void keepsExistingConfigurationWhenStatementRefreshReturnsNull() {
        TestingExecutor executor = new TestingExecutor();
        executor.setColorSchema(DARK);

        SqlClientSyntaxHighlighter highlighter = new SqlClientSyntaxHighlighter(executor);
        SqlMultiLineParser parser =
                new SqlMultiLineParser(
                        new SqlCommandParserImpl(),
                        executor,
                        CliClient.ExecutionMode.INTERACTIVE_EXECUTION,
                        highlighter::updateSessionConfig);

        executor.returnNullSessionConfigAfterExecute();

        parser.parse(SET_COLOR_SCHEMA, SET_COLOR_SCHEMA.length(), Parser.ParseContext.ACCEPT_LINE);

        assertThat(executor.executedStatement).isEqualTo(SET_COLOR_SCHEMA);
        assertHighlightedAs(highlighter, DARK);
        assertThat(executor.getSessionConfigCalls).isEqualTo(3);
    }

    private static void assertHighlightedAs(
            SqlClientSyntaxHighlighter highlighter, SyntaxHighlightStyle.BuiltInStyle style) {
        assertThat(highlighter.highlight(null, SQL).toAnsi())
                .isEqualTo(
                        SqlClientSyntaxHighlighter.getHighlightedOutput(
                                        SQL, style.getHighlightStyle(), SqlDialect.DEFAULT)
                                .toAnsi());
    }

    private static class TestingExecutor implements Executor {

        private final Configuration configuration = new Configuration();
        private int getSessionConfigCalls;
        private SyntaxHighlightStyle.BuiltInStyle colorSchemaAfterExecute;
        private boolean returnNullSessionConfigAfterExecute;
        private boolean returnNullSessionConfig;
        private String executedStatement;

        private void setColorSchema(SyntaxHighlightStyle.BuiltInStyle style) {
            configuration.set(SqlClientOptions.DISPLAY_DEFAULT_COLOR_SCHEMA, style.name());
        }

        private void setColorSchemaAfterExecute(SyntaxHighlightStyle.BuiltInStyle style) {
            colorSchemaAfterExecute = style;
        }

        private void returnNullSessionConfigAfterExecute() {
            returnNullSessionConfigAfterExecute = true;
        }

        @Override
        public void configureSession(String statement) {}

        @Override
        public ReadableConfig getSessionConfig() {
            getSessionConfigCalls++;
            if (returnNullSessionConfig) {
                return null;
            }
            return Configuration.fromMap(configuration.toMap());
        }

        @Override
        public Map<String, String> getSessionConfigMap() {
            return configuration.toMap();
        }

        @Override
        public StatementResult executeStatement(String statement) {
            executedStatement = statement;
            if (colorSchemaAfterExecute != null) {
                setColorSchema(colorSchemaAfterExecute);
            }
            if (returnNullSessionConfigAfterExecute) {
                returnNullSessionConfig = true;
            }
            return new StatementResult(
                    ResolvedSchema.of(),
                    CloseableIterator.empty(),
                    false,
                    ResultKind.SUCCESS,
                    null);
        }

        @Override
        public List<String> completeStatement(String statement, int position) {
            return Collections.emptyList();
        }

        @Override
        public String deployScript(@Nullable String script, @Nullable URI uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
