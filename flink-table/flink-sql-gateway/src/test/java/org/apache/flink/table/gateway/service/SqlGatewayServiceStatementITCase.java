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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.gateway.service.utils.SqlScriptReader;
import org.apache.flink.table.gateway.service.utils.TestSqlStatement;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.flink.shaded.guava30.com.google.common.io.PatternFilenameFilter;

import org.apache.calcite.util.Util;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.gateway.service.utils.SqlScriptReader.HINT_START_OF_OUTPUT;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceNodeIdInOperator;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStreamNodeId;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Test {@link SqlGatewayService}#executeStatement. */
@Execution(CONCURRENT)
public class SqlGatewayServiceStatementITCase {

    @RegisterExtension
    @Order(1)
    public static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    @RegisterExtension
    @Order(2)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    private static SqlGatewayService service;

    private final SessionEnvironment defaultSessionEnvironment =
            SessionEnvironment.newBuilder()
                    .setSessionEndpointVersion(MockedEndpointVersion.V1)
                    .build();
    private final Map<String, String> replaceVars = new HashMap<>();

    @BeforeAll
    public static void setUp() {
        service = SQL_GATEWAY_SERVICE_EXTENSION.getService();
    }

    @BeforeEach
    public void before(@TempDir Path temporaryFolder) throws IOException {
        // initialize new folders for every test, so the vars can be reused by every SQL scripts
        replaceVars.put(
                "$VAR_STREAMING_PATH",
                Files.createDirectory(temporaryFolder.resolve("streaming")).toFile().getPath());
        replaceVars.put(
                "$VAR_BATCH_PATH",
                Files.createDirectory(temporaryFolder.resolve("batch")).toFile().getPath());
    }

    public static Stream<String> parameters() throws Exception {
        String first = "sql/table.q";
        URL url = SqlGatewayServiceStatementITCase.class.getResource("/" + first);
        File firstFile = Paths.get(checkNotNull(url.toURI())).toFile();
        final int commonPrefixLength = firstFile.getAbsolutePath().length() - first.length();
        File dir = firstFile.getParentFile();
        final List<String> paths = new ArrayList<>();
        final FilenameFilter filter = new PatternFilenameFilter(".*\\.q$");
        for (File f : Util.first(dir.listFiles(filter), new File[0])) {
            paths.add(f.getAbsolutePath().substring(commonPrefixLength));
        }
        return paths.stream();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testSqlStatements(String sqlPath) throws Exception {
        String in = getInputFromPath(sqlPath);
        List<TestSqlStatement> testSqlStatements = SqlScriptReader.parseSqlScript(in);

        assertThat(String.join("", runStatements(testSqlStatements))).isEqualTo(in);
    }

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param statements the SQL statements to run
     * @return the stringified results
     */
    private List<String> runStatements(List<TestSqlStatement> statements) {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        List<String> output = new ArrayList<>();
        for (TestSqlStatement statement : statements) {
            StringBuilder builder = new StringBuilder();
            builder.append(statement.getComment());
            builder.append(statement.getSql());

            try {
                builder.append(runSingleStatement(sessionHandle, statement.getSql()));
            } catch (Throwable t) {
                Throwable root = getRootCause(t);
                builder.append(
                        Tag.ERROR.addTag(
                                root.getClass().getName()
                                        + ": "
                                        + root.getMessage().trim()
                                        + "\n"));
            }
            output.add(builder.toString());
        }

        return output;
    }

    // -------------------------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------------------------

    /** Mark the output type. */
    enum Tag {
        INFO("!info"),

        OK("!ok"),

        ERROR("!error");

        private final String tag;

        Tag(String tag) {
            this.tag = tag;
        }

        public String addTag(String content) {
            return HINT_START_OF_OUTPUT + "\n" + content + tag + "\n";
        }
    }

    private String getInputFromPath(String sqlPath) throws IOException {
        URL url = SqlGatewayServiceStatementITCase.class.getResource("/" + sqlPath);

        // replace the placeholder with specified value if exists
        String[] keys = replaceVars.keySet().toArray(new String[0]);
        String[] values = Arrays.stream(keys).map(replaceVars::get).toArray(String[]::new);

        return StringUtils.replaceEach(
                IOUtils.toString(checkNotNull(url), StandardCharsets.UTF_8), keys, values);
    }

    protected static Throwable getRootCause(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param sessionHandle the Session that run the statement
     * @param statement the SQL statement to run
     * @return the printed results in tableau style
     */
    private String runSingleStatement(SessionHandle sessionHandle, String statement)
            throws Exception {
        OperationHandle operationHandle =
                service.executeStatement(sessionHandle, statement, -1, new Configuration());
        CommonTestUtils.waitUtil(
                () ->
                        service.getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(100),
                "Failed to wait operation finish.");

        if (!service.getOperationInfo(sessionHandle, operationHandle).isHasResults()) {
            return Tag.INFO.addTag("");
        }

        // The content in the result of the `explain` and `show create` statement is large, so it's
        // more straightforward to just print the content without the table.
        if (statement.toUpperCase().startsWith("EXPLAIN")
                || statement.toUpperCase().startsWith("SHOW CREATE")) {
            ResultSet resultSet =
                    service.fetchResults(sessionHandle, operationHandle, 0, Integer.MAX_VALUE);
            return Tag.OK.addTag(
                    replaceStreamNodeId(
                                    replaceNodeIdInOperator(
                                            resultSet.getData().get(0).getString(0).toString()))
                            + "\n");
        } else {
            ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            ResultSet resultSet =
                    service.fetchResults(sessionHandle, operationHandle, 0, Integer.MAX_VALUE);

            boolean isStreaming =
                    Configuration.fromMap(service.getSessionConfig(sessionHandle))
                            .get(RUNTIME_MODE)
                            .equals(STREAMING);
            boolean isQuery = statement.toUpperCase().startsWith("SELECT");

            PrintStyle style =
                    PrintStyle.tableauWithDataInferredColumnWidths(
                            resultSet.getResultSchema(),
                            new RowDataToStringConverterImpl(
                                    resultSet.getResultSchema().toPhysicalRowDataType(),
                                    DateTimeUtils.UTC_ZONE.toZoneId(),
                                    SqlGatewayServiceStatementITCase.class.getClassLoader(),
                                    false),
                            Integer.MAX_VALUE,
                            true,
                            isStreaming && isQuery);

            PrintWriter writer = new PrintWriter(outContent);
            style.print(new RowDataIterator(sessionHandle, operationHandle), writer);
            return Tag.OK.addTag(outContent.toString());
        }
    }

    private static class RowDataIterator implements Iterator<RowData> {

        private final SessionHandle sessionHandle;
        private final OperationHandle operationHandle;

        private Long token = 0L;
        private Iterator<RowData> fetchedRows = Collections.emptyIterator();

        public RowDataIterator(SessionHandle sessionHandle, OperationHandle operationHandle) {
            this.sessionHandle = sessionHandle;
            this.operationHandle = operationHandle;
        }

        @Override
        public boolean hasNext() {
            while (token != null && !fetchedRows.hasNext()) {
                ResultSet resultSet =
                        service.fetchResults(
                                sessionHandle, operationHandle, token, Integer.MAX_VALUE);
                token = resultSet.getNextToken();
                fetchedRows = resultSet.getData().iterator();
            }

            return token != null;
        }

        @Override
        public RowData next() {
            return fetchedRows.next();
        }
    }
}
