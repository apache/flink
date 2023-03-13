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

package org.apache.flink.table.gateway.service;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.AbstractSqlGatewayStatementITCase;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.utils.DateTimeUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.BiFunction;

import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.createInitializedSession;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchResults;
import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link SqlGatewayService}#executeStatement. */
public class SqlGatewayServiceStatementITCase extends AbstractSqlGatewayStatementITCase {

    private final SessionEnvironment defaultSessionEnvironment =
            SessionEnvironment.newBuilder()
                    .setSessionEndpointVersion(MockedEndpointVersion.V1)
                    .build();

    private SessionHandle sessionHandle;

    @BeforeEach
    @Override
    public void before(@TempDir Path temporaryFolder) throws Exception {
        super.before(temporaryFolder);
        sessionHandle = service.openSession(defaultSessionEnvironment);
    }

    @Override
    protected String runSingleStatement(String statement) throws Exception {
        OperationHandle operationHandle =
                service.executeStatement(sessionHandle, statement, -1, new Configuration());
        CommonTestUtils.waitUtil(
                () ->
                        service.getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(100),
                "Failed to wait operation finish.");

        ResultSet resultSet =
                service.fetchResults(sessionHandle, operationHandle, 0, Integer.MAX_VALUE);

        return toString(
                StatementType.match(statement),
                resultSet.getResultSchema(),
                new RowDataToStringConverterImpl(
                        resultSet.getResultSchema().toPhysicalRowDataType(),
                        DateTimeUtils.UTC_ZONE.toZoneId(),
                        SqlGatewayServiceStatementITCase.class.getClassLoader(),
                        false),
                new RowDataIterator(sessionHandle, operationHandle));
    }

    @Override
    protected String stringifyException(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null
                && root.getCause().getMessage() != null
                && !root.getCause().getMessage().isEmpty()) {
            root = root.getCause();
        }
        return root.getClass().getName() + ": " + root.getMessage();
    }

    @Override
    protected boolean isStreaming() {
        return Configuration.fromMap(service.getSessionConfig(sessionHandle))
                .get(ExecutionOptions.RUNTIME_MODE)
                .equals(RuntimeExecutionMode.STREAMING);
    }

    private static class RowDataIterator implements Iterator<RowData> {

        private final SessionHandle sessionHandle;
        private final OperationHandle operationHandle;

        private Long token = 0L;
        private Iterator<RowData> fetchedRows = Collections.emptyIterator();

        public RowDataIterator(SessionHandle sessionHandle, OperationHandle operationHandle) {
            this.sessionHandle = sessionHandle;
            this.operationHandle = operationHandle;
            fetch();
        }

        @Override
        public boolean hasNext() {
            while (token != null && !fetchedRows.hasNext()) {
                fetch();
            }

            return token != null;
        }

        @Override
        public RowData next() {
            return fetchedRows.next();
        }

        private void fetch() {
            ResultSet resultSet =
                    service.fetchResults(sessionHandle, operationHandle, token, Integer.MAX_VALUE);
            token = resultSet.getNextToken();
            fetchedRows = resultSet.getData().iterator();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Validate ResultSet fields
    // --------------------------------------------------------------------------------------------

    @Test
    void testIsQueryResult() throws Exception {
        SessionHandle sessionHandle = createInitializedSession(service);

        BiFunction<SessionHandle, OperationHandle, Boolean> isQueryResultGetter =
                (sessionHandle1, operationHandle) ->
                        fetchResults(service, sessionHandle1, operationHandle).isQueryResult();

        // trivial query syntax
        validateResultSetField(
                sessionHandle, "SELECT * FROM cat1.db1.tbl1;", isQueryResultGetter, true);

        // query with CTE
        validateResultSetField(
                sessionHandle,
                "WITH hub AS (SELECT * FROM cat1.db1.tbl1)\nSELECT * FROM hub;",
                isQueryResultGetter,
                true);

        // non-query
        validateResultSetField(
                sessionHandle,
                "INSERT INTO cat1.db1.tbl1 SELECT * FROM cat1.db1.tbl2;",
                isQueryResultGetter,
                false);
    }

    @Test
    void testHasJobID() throws Exception {
        SessionHandle sessionHandle = createInitializedSession(service);

        BiFunction<SessionHandle, OperationHandle, Boolean> hasJobIDGetter =
                (sessionHandle1, operationHandle) ->
                        fetchResults(service, sessionHandle1, operationHandle).getJobID() != null;

        // query
        validateResultSetField(sessionHandle, "SELECT * FROM cat1.db1.tbl1;", hasJobIDGetter, true);

        // insert
        validateResultSetField(
                sessionHandle,
                "INSERT INTO cat1.db1.tbl1 SELECT * FROM cat1.db1.tbl2;",
                hasJobIDGetter,
                true);

        // ddl
        validateResultSetField(
                sessionHandle,
                "CREATE TABLE test (f0 INT) WITH ('connector' = 'values');",
                hasJobIDGetter,
                false);
    }

    @Test
    void testResultKind() throws Exception {
        SessionHandle sessionHandle = createInitializedSession(service);

        BiFunction<SessionHandle, OperationHandle, ResultKind> resultKindGetter =
                (sessionHandle1, operationHandle) ->
                        fetchResults(service, sessionHandle1, operationHandle).getResultKind();

        // query
        validateResultSetField(
                sessionHandle,
                "SELECT * FROM cat1.db1.tbl1;",
                resultKindGetter,
                ResultKind.SUCCESS_WITH_CONTENT);

        // insert
        validateResultSetField(
                sessionHandle,
                "INSERT INTO cat1.db1.tbl1 SELECT * FROM cat1.db1.tbl2;",
                resultKindGetter,
                ResultKind.SUCCESS_WITH_CONTENT);

        // ddl
        validateResultSetField(
                sessionHandle,
                "CREATE TABLE test (f0 INT) WITH ('connector' = 'values');",
                resultKindGetter,
                ResultKind.SUCCESS);

        // set
        validateResultSetField(
                sessionHandle, "SET 'key' = 'value';", resultKindGetter, ResultKind.SUCCESS);

        validateResultSetField(
                sessionHandle, "SET;", resultKindGetter, ResultKind.SUCCESS_WITH_CONTENT);
    }

    private <T> void validateResultSetField(
            SessionHandle sessionHandle,
            String statement,
            BiFunction<SessionHandle, OperationHandle, T> resultGetter,
            T expected)
            throws Exception {
        OperationHandle operationHandle =
                service.executeStatement(sessionHandle, statement, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, operationHandle);
        assertThat(resultGetter.apply(sessionHandle, operationHandle)).isEqualTo(expected);
    }
}
