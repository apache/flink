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

package org.apache.flink.table.client.cli;

import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the exception line that {@link CliStrings#messageError} picks. */
class CliStringsTest {

    private static final String NON_QUERY = "Non-query expression encountered in illegal context";
    private static final String POSITIONED_PARSE_ERROR =
            "SQL parse failed. From line 1, column 1 to line 1, column 5:\n"
                    + "    ELECT 1\n"
                    + "    ^^^^^\n"
                    + NON_QUERY;

    private static final String SERVER_PREAMBLE =
            "[Internal server error., <Exception on server side:\n";
    private static final String SERVER_END = "End of exception on server side>]";

    // The chain a parse error takes from the gateway to the client, as the server prints it.
    private static final String PARSE_ERROR_BODY =
            SERVER_PREAMBLE
                    + "org.apache.flink.table.gateway.api.utils.SqlGatewayException: "
                    + "org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults.\n"
                    + "\tat org.apache.flink.table.gateway.rest.handler.statement.FetchResultsHandler.handleRequest(FetchResultsHandler.java:91)\n"
                    + "Caused by: org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults.\n"
                    + "\tat org.apache.flink.table.gateway.service.SqlGatewayServiceImpl.fetchResults(SqlGatewayServiceImpl.java:238)\n"
                    + "\t... 3 more\n"
                    + "Caused by: org.apache.flink.table.gateway.service.utils.SqlExecutionException: Failed to execute the operation 1.\n"
                    + "\tat org.apache.flink.table.gateway.service.operation.OperationManager.processThrowable(OperationManager.java:412)\n"
                    + "Caused by: org.apache.flink.table.api.SqlParserException: "
                    + POSITIONED_PARSE_ERROR
                    + "\n"
                    + "\tat org.apache.flink.table.planner.parse.CalciteParser.parse(CalciteParser.java:60)\n"
                    + "Caused by: org.apache.calcite.sql.parser.SqlParseException: "
                    + NON_QUERY
                    + "\n"
                    + "\tat org.apache.flink.sql.parser.impl.FlinkSqlParserImpl.convertException(FlinkSqlParserImpl.java:512)\n"
                    + "Caused by: org.apache.calcite.runtime.CalciteException: "
                    + NON_QUERY
                    + "\n"
                    + "\tat java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)\n"
                    + "\t... 12 more\n"
                    + SERVER_END;

    @Test
    void testChainShowsTheOutermostExceptionThatQuotesTheRootCause() {
        final Throwable chain =
                new SqlExecutionException(
                        "Failed to execute the operation 1.",
                        new SqlParserException(
                                POSITIONED_PARSE_ERROR, new RuntimeException(NON_QUERY)));

        assertThat(reason(chain))
                .isEqualTo(
                        "org.apache.flink.table.api.SqlParserException: " + POSITIONED_PARSE_ERROR);
    }

    @Test
    void testChainShowsTheStatementThatFailed() {
        final Throwable chain =
                new SqlExecutionException(
                        "Failed to execute the operation 1.",
                        new ValidationException(
                                "Could not execute LOAD MODULE core. A module with name 'core' already exists",
                                new ValidationException(
                                        "A module with name 'core' already exists")));

        assertThat(reason(chain))
                .isEqualTo(
                        "org.apache.flink.table.api.ValidationException: Could not execute LOAD MODULE"
                                + " core. A module with name 'core' already exists");
    }

    @Test
    void testChainShowsTheRootCauseWhenNothingQuotesIt() {
        final Throwable chain =
                new SqlExecutionException(
                        "Failed to execute the operation 1.", new RuntimeException("boom"));

        assertThat(reason(chain)).isEqualTo("java.lang.RuntimeException: boom");
    }

    @Test
    void testChainIgnoresAWrapperWithTheSameMessage() {
        final Throwable chain = new ValidationException("boom", new RuntimeException("boom"));

        assertThat(reason(chain)).isEqualTo("java.lang.RuntimeException: boom");
    }

    @Test
    void testChainIgnoresAWrapperThatOnlyAddsItsClassName() {
        final Throwable chain = new RuntimeException(new IllegalStateException("boom"));

        assertThat(reason(chain)).isEqualTo("java.lang.IllegalStateException: boom");
    }

    @Test
    void testChainStopsAtAnEmptyMessage() {
        final Throwable chain = new RuntimeException("outer", new IllegalStateException(""));

        assertThat(reason(chain)).isEqualTo("java.lang.RuntimeException: outer");
    }

    @Test
    void testChainKeepsTheIncompleteStatementLine() {
        final String body =
                SERVER_PREAMBLE
                        + "org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults.\n"
                        + "\tat org.apache.flink.table.gateway.service.SqlGatewayServiceImpl.fetchResults(SqlGatewayServiceImpl.java:238)\n"
                        + "Caused by: org.apache.flink.sql.parser.impl.ParseException: Encountered \"<EOF>\" at line 1, column 13.\n"
                        + "Was expecting one of:\n"
                        + "    \"AS\" ...\n"
                        + "    \n"
                        + "\tat org.apache.flink.sql.parser.impl.FlinkSqlParserImpl.generateParseException(FlinkSqlParserImpl.java:1)\n"
                        + SERVER_END;
        // ExecutorImpl repeats the whole body in the wrapper it builds for an incomplete statement.
        final Throwable chain =
                new SqlExecutionException(
                        "The SQL statement is incomplete.",
                        new SqlParserEOFException(
                                body,
                                new RestClientException(
                                        body, HttpResponseStatus.INTERNAL_SERVER_ERROR)));

        assertThat(reason(chain))
                .isEqualTo(
                        "org.apache.flink.sql.parser.impl.ParseException: Encountered \"<EOF>\" at"
                                + " line 1, column 13.\nWas expecting one of:\n    \"AS\" ...");
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testCyclicCausesTerminate() {
        final RuntimeException inner = new RuntimeException("b");
        final RuntimeException outer = new RuntimeException("a", inner);
        inner.initCause(outer);

        assertThat(reason(outer)).isEqualTo("java.lang.RuntimeException: b");
    }

    @Test
    void testRestBodyShowsTheOutermostSegmentThatQuotesTheRootCause() {
        assertThat(reason(restFailure(PARSE_ERROR_BODY)))
                .isEqualTo(
                        "org.apache.flink.table.api.SqlParserException: " + POSITIONED_PARSE_ERROR);
    }

    @Test
    void testRestBodyShowsTheRootCauseWhenNothingQuotesIt() {
        final String body =
                "[org.apache.flink.runtime.rest.handler.RestHandlerException: Failed to cancelOperation.\n"
                        + "\tat org.apache.flink.table.gateway.rest.handler.operation.AbstractOperationHandler.handleRequest(AbstractOperationHandler.java:79)\n"
                        + "Caused by: java.lang.RuntimeException: boom\n"
                        + "\tat org.apache.flink.table.gateway.service.operation.OperationManager.cancelOperation(OperationManager.java:1)\n"
                        + "]";

        assertThat(reason(restFailure(body))).isEqualTo("java.lang.RuntimeException: boom");
    }

    @Test
    void testRestBodyWithoutCausesIsShownAsIs() {
        final String body = "[The endpoint has not started yet.]";

        assertThat(reason(restFailure(body))).isEqualTo(body);
    }

    @Test
    void testRestBodyCutsTheRootCauseAtItsFrames() {
        final String body =
                SERVER_PREAMBLE
                        + "org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults.\n"
                        + "\tat org.apache.flink.table.gateway.service.SqlGatewayServiceImpl.fetchResults(SqlGatewayServiceImpl.java:238)\n"
                        + "Caused by: java.lang.RuntimeException: boom\n"
                        + "\t... 3 more\n"
                        + SERVER_END;

        assertThat(reason(restFailure(body))).isEqualTo("java.lang.RuntimeException: boom");
    }

    @Test
    void testRestBodyIgnoresACauseCaptionInsideAMessage() {
        final String notFound = "Column 'Caused by: x' not found in any table";
        final String positioned = "From line 1, column 8 to line 1, column 21: " + notFound;
        final String body =
                SERVER_PREAMBLE
                        + "org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults.\n"
                        + "\tat org.apache.flink.table.gateway.service.SqlGatewayServiceImpl.fetchResults(SqlGatewayServiceImpl.java:238)\n"
                        + "Caused by: org.apache.flink.table.api.ValidationException: SQL validation failed. "
                        + positioned
                        + "\n"
                        + "\tat org.apache.flink.table.planner.calcite.FlinkPlannerImpl.validate(FlinkPlannerImpl.scala:205)\n"
                        + "Caused by: org.apache.calcite.runtime.CalciteContextException: "
                        + positioned
                        + "\n"
                        + "\tat java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)\n"
                        + "Caused by: org.apache.calcite.sql.validate.SqlValidatorException: "
                        + notFound
                        + "\n"
                        + "\tat java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)\n"
                        + SERVER_END;

        assertThat(reason(restFailure(body)))
                .isEqualTo(
                        "org.apache.flink.table.api.ValidationException: SQL validation failed. "
                                + positioned);
    }

    @Test
    void testVerboseOutputStartsAtTheChosenException() {
        final Throwable chain =
                new SqlExecutionException(
                        "Failed to execute the operation 1.",
                        new SqlParserException(
                                POSITIONED_PARSE_ERROR, new RuntimeException(NON_QUERY)));

        final String output =
                CliStrings.messageError("Could not execute SQL statement.", chain, true).toString();

        assertThat(output)
                .contains(
                        "org.apache.flink.table.api.SqlParserException: " + POSITIONED_PARSE_ERROR)
                .contains("Caused by: java.lang.RuntimeException: " + NON_QUERY)
                .doesNotContain("Failed to execute the operation 1.");
    }

    private static Throwable restFailure(String body) {
        return new SqlExecutionException(
                "Failed to get response for the operation 1.",
                new RestClientException(body, HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }

    private static String reason(Throwable t) {
        final String output =
                CliStrings.messageError("Could not execute SQL statement.", t, false).toString();
        final String header = "[ERROR] Could not execute SQL statement. Reason:\n";
        assertThat(output).startsWith(header);
        return output.substring(header.length());
    }
}
