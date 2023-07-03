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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.client.cli.parser.SqlCommandParserImpl;
import org.apache.flink.table.client.cli.parser.SqlMultiLineParser;
import org.apache.flink.table.client.cli.utils.SqlScriptReader;
import org.apache.flink.table.client.cli.utils.TestSqlStatement;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SingleSessionManager;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.planner.factories.TestUpdateDeleteTableFactory;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.test.junit5.InjectClusterClientConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.apache.flink.shaded.guava31.com.google.common.io.PatternFilenameFilter;

import org.apache.calcite.util.Util;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.repeat;
import static org.apache.flink.configuration.JobManagerOptions.ADDRESS;
import static org.apache.flink.configuration.RestOptions.PORT;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test that runs every {@code xx.q} file in "resources/sql/" path as a test. */
class CliClientITCase {

    private static Path historyPath;
    private static Map<String, String> replaceVars;

    @TempDir private static Path tempFolder;

    @RegisterExtension
    @Order(1)
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @RegisterExtension
    @Order(2)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(
                    MINI_CLUSTER_RESOURCE::getClientConfiguration, SingleSessionManager::new);

    @RegisterExtension
    @Order(3)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    static Stream<String> sqlPaths() throws Exception {
        String first = "sql/table.q";
        URL url = CliClientITCase.class.getResource("/" + first);
        File firstFile = Paths.get(url.toURI()).toFile();
        final int commonPrefixLength = firstFile.getAbsolutePath().length() - first.length();
        File dir = firstFile.getParentFile();
        final List<String> paths = new ArrayList<>();
        final FilenameFilter filter = new PatternFilenameFilter(".*\\.q$");
        for (File f : Util.first(dir.listFiles(filter), new File[0])) {
            paths.add(f.getAbsolutePath().substring(commonPrefixLength));
        }
        return paths.stream();
    }

    @BeforeAll
    static void setup(@InjectClusterClientConfiguration Configuration configuration)
            throws IOException {
        Map<String, String> classNameCodes = new HashMap<>();
        classNameCodes.put(
                GENERATED_LOWER_UDF_CLASS,
                String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
        classNameCodes.put(
                GENERATED_UPPER_UDF_CLASS,
                String.format(GENERATED_UPPER_UDF_CODE, GENERATED_UPPER_UDF_CLASS));

        File udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        Files.createTempDirectory(tempFolder, "test-jar").toFile(),
                        "test-classloader-udf.jar",
                        classNameCodes);
        URL udfDependency = udfJar.toURI().toURL();
        String path = udfDependency.getPath();
        // we need to pad the displayed "jars" tableau to have the same width of path string
        // 4 for the "jars" characters, see `set.q` test file
        int paddingLen = path.length() - 4;
        historyPath = Files.createTempFile(tempFolder, "history", "");

        replaceVars = new HashMap<>();
        replaceVars.put("$VAR_UDF_JAR_PATH", path);
        replaceVars.put("$VAR_UDF_JAR_PATH_DASH", repeat('-', paddingLen));
        replaceVars.put("$VAR_UDF_JAR_PATH_SPACE", repeat(' ', paddingLen));
        replaceVars.put("$VAR_PIPELINE_JARS_URL", udfDependency.toString());
        replaceVars.put("$VAR_REST_PORT", configuration.get(PORT).toString());
        replaceVars.put("$VAR_JOBMANAGER_RPC_ADDRESS", configuration.get(ADDRESS));
        replaceVars.put("$VAR_DELETE_TABLE_DATA_ID", prepareData());
        replaceVars.put("$VAR_TRUNCATE_TABLE_DATA_ID", prepareData());
    }

    @BeforeEach
    void before() throws IOException {
        // initialize new folders for every test, so the vars can be reused by every SQL script
        replaceVars.put(
                "$VAR_STREAMING_PATH",
                Files.createTempDirectory(tempFolder, UUID.randomUUID().toString()).toString());
        replaceVars.put(
                "$VAR_STREAMING_PATH2",
                Files.createTempDirectory(tempFolder, UUID.randomUUID().toString()).toString());
        replaceVars.put(
                "$VAR_BATCH_PATH",
                Files.createTempDirectory(tempFolder, UUID.randomUUID().toString()).toString());
        replaceVars.put(
                "$VAR_BATCH_PATH2",
                Files.createTempDirectory(tempFolder, UUID.randomUUID().toString()).toString());
    }

    @ParameterizedTest
    @MethodSource("sqlPaths")
    void testSqlStatements(
            String sqlPath, @InjectClusterClientConfiguration Configuration configuration)
            throws IOException {
        String in = getInputFromPath(sqlPath);
        List<TestSqlStatement> testSqlStatements = parseSqlScript(in);
        List<String> sqlStatements =
                testSqlStatements.stream().map(s -> s.sql).collect(Collectors.toList());
        List<Result> actualResults = runSqlStatements(sqlStatements, configuration);
        String out = transformOutput(testSqlStatements, actualResults);
        assertThat(out).isEqualTo(in);
    }

    @ParameterizedTest
    @MethodSource("tableOptionToJlineVarProvider")
    void testPropagationOfTableOptionToVars(
            String setTableOptionCommand,
            String jlineVarName,
            String expectedValue,
            @InjectClusterClientConfiguration Configuration configuration)
            throws IOException {

        DefaultContext defaultContext =
                new DefaultContext(
                        new Configuration(configuration)
                                // Make sure we use the new cast behaviour
                                .set(
                                        ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR,
                                        ExecutionConfigOptions.LegacyCastBehaviour.DISABLED),
                        Collections.emptyList());

        // Since DumbTerminal exits automatically with the last line need to add \n to have command
        // processed before the exit
        InputStream inputStream =
                new ByteArrayInputStream((setTableOptionCommand + "\n").getBytes());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(256);

        try (final Executor executor =
                        Executor.create(
                                defaultContext,
                                InetSocketAddress.createUnresolved(
                                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()),
                                "test-session");
                Terminal terminal = new DumbTerminal(inputStream, outputStream);
                CliClient client =
                        new CliClient(
                                () -> terminal, executor, historyPath, HideSqlStatement.INSTANCE)) {

            LineReader dummyLineReader =
                    LineReaderBuilder.builder()
                            .terminal(terminal)
                            .parser(
                                    new SqlMultiLineParser(
                                            new SqlCommandParserImpl(),
                                            executor,
                                            CliClient.ExecutionMode.INTERACTIVE_EXECUTION))
                            .build();
            client.executeInInteractiveMode(dummyLineReader);
            assertThat(dummyLineReader.getVariable(jlineVarName)).isEqualTo(expectedValue);
        }
    }

    static Stream<Arguments> tableOptionToJlineVarProvider() {
        return Stream.of(
                Arguments.of(
                        "SET 'sql-client.display.show-line-numbers' = 'true';",
                        LineReader.SECONDARY_PROMPT_PATTERN,
                        "%N%M> "));
    }

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param statements the SQL statements to run
     * @return the printed results on SQL Client
     */
    private List<Result> runSqlStatements(List<String> statements, Configuration configuration)
            throws IOException {
        final String sqlContent = String.join("", statements);
        DefaultContext defaultContext =
                new DefaultContext(
                        new Configuration(configuration)
                                // Make sure we use the new cast behaviour
                                .set(
                                        ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR,
                                        ExecutionConfigOptions.LegacyCastBehaviour.DISABLED),
                        Collections.emptyList());

        InputStream inputStream = new ByteArrayInputStream(sqlContent.getBytes());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(256);

        try (final Executor executor =
                        Executor.create(
                                defaultContext,
                                InetSocketAddress.createUnresolved(
                                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()),
                                "test-session");
                Terminal terminal = new DumbTerminal(inputStream, outputStream);
                CliClient client =
                        new CliClient(
                                () -> terminal, executor, historyPath, HideSqlStatement.INSTANCE)) {
            client.executeInInteractiveMode();
            String output = new String(outputStream.toByteArray());
            return normalizeOutput(output);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------------------------

    private static final String PROMOTE = "Flink SQL> ";
    private static final String JOB_ID = "Job ID:";

    enum Tag {
        ERROR("\u001B[31;1m", "\u001B[0m", "!error"),

        WARNING("\u001B[33;1m", "\u001B[0m", "!warning"),

        INFO("\u001B[34;1m", "\u001B[0m", "!info"),

        OK("", "", "!ok");

        public final String begin;
        public final String end;
        public final String tag;

        Tag(String begin, String end, String tag) {
            this.begin = begin;
            this.end = end;
            this.tag = tag;
        }

        public boolean matches(List<String> lines) {
            return containsTag(lines, begin) && containsTag(lines, end);
        }

        public List<String> convert(List<String> lines) {
            List<String> newLines = new ArrayList<>();
            for (String line : lines) {
                String newLine =
                        StringUtils.replaceEach(
                                line, new String[] {begin, end}, new String[] {"", ""});

                // there might be trailing white spaces,
                // we should remove them because we don't compare trailing white spaces
                newLines.add(StringUtils.stripEnd(newLine, " "));
            }
            return newLines;
        }

        private boolean containsTag(List<String> contents, String tag) {
            for (String content : contents) {
                if (content.contains(tag)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static String prepareData() {
        List<RowData> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            values.add(GenericRowData.of(i, StringData.fromString("b_" + i), i * 2.0));
        }
        return TestUpdateDeleteTableFactory.registerRowData(values);
    }

    private static String getInputFromPath(String sqlPath) throws IOException {
        URL url = CliClientITCase.class.getResource("/" + sqlPath);
        String in = IOUtils.toString(url, StandardCharsets.UTF_8);

        // replace the placeholder with specified value if exists
        String[] keys = replaceVars.keySet().toArray(new String[0]);
        String[] values = Arrays.stream(keys).map(replaceVars::get).toArray(String[]::new);

        return StringUtils.replaceEach(in, keys, values);
    }

    protected List<TestSqlStatement> parseSqlScript(String input) {
        return SqlScriptReader.parseSqlScript(input);
    }

    private static List<Result> normalizeOutput(String output) {
        List<Result> results = new ArrayList<>();
        // remove welcome message
        String str = output.substring(output.indexOf(PROMOTE));
        List<String> contentLines = new ArrayList<>();
        boolean reachFirstPromote = false;
        try (BufferedReader reader = new BufferedReader(new StringReader(str))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(PROMOTE)) {
                    if (reachFirstPromote) {
                        // begin a new result, put current result into list
                        results.add(convertToResult(contentLines));
                        // and start to consume new one
                        contentLines.clear();
                    } else {
                        // start to consume a new result
                        reachFirstPromote = true;
                    }
                    // remove the promote prefix
                    line = line.substring(PROMOTE.length());
                }
                // ignore the line begin with Job ID:
                if (!line.startsWith(JOB_ID)) {
                    contentLines.add(line);
                } else {
                    contentLines.add(JOB_ID);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // remove last empty result if exists, because it will always print
        // an additional "Flink SQL>" at the end
        int last = results.size() - 1;
        if (results.size() > 0 && results.get(last).content.isEmpty()) {
            results.remove(last);
        }
        return results;
    }

    private static Result convertToResult(List<String> contentLines) {
        List<Tag> tags = new ArrayList<>();

        for (Tag tag : Tag.values()) {
            if (tag.matches(contentLines)) {
                tags.add(tag);
            }
        }

        String content = stripTagsAndConcatLines(contentLines, tags);
        return new Result(content, tags.get(0));
    }

    private static String stripTagsAndConcatLines(List<String> lines, List<Tag> tags) {
        List<String> newLines = lines;
        for (Tag tag : tags) {
            newLines = tag.convert(newLines);
        }
        return String.join("\n", newLines);
    }

    protected String transformOutput(
            List<TestSqlStatement> testSqlStatements, List<Result> results) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < testSqlStatements.size(); i++) {
            TestSqlStatement sqlScript = testSqlStatements.get(i);
            out.append(sqlScript.comment).append(sqlScript.sql);
            if (i < results.size()) {
                Result result = results.get(i);
                String content =
                        TableTestUtil.replaceNodeIdInOperator(removeExecNodeId(result.content));

                int removedChatNumber = result.content.length() - content.length();
                String borderLineStart = "+-";
                String columnStart = "|";
                for (int j = 0; j < removedChatNumber; j++) {
                    borderLineStart += "-";
                    columnStart += " ";
                }
                content = content.replaceAll("\\" + borderLineStart, "+-");
                content = content.replace(columnStart, "|");

                out.append(content).append(result.highestTag.tag).append("\n");
            }
        }

        return out.toString();
    }

    protected static String removeExecNodeId(String s) {
        return s.replaceAll("\"id\" : \\d+", "\"id\" : ");
    }

    /** test result. */
    protected static final class Result {
        final String content;
        final Tag highestTag;

        private Result(String content, Tag highestTag) {
            this.highestTag = highestTag;
            this.content = content;
        }
    }

    private static final class HideSqlStatement implements MaskingCallback {

        private static final MaskingCallback INSTANCE = new HideSqlStatement();

        @Override
        public String display(String line) {
            // do not display input command
            return "";
        }

        @Override
        public String history(String line) {
            return line;
        }
    }
}
