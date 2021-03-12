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

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.cli.utils.TestSqlStatement;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.apache.flink.table.client.gateway.utils.TestUserClassLoaderJar;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.flink.shaded.guava18.com.google.common.io.PatternFilenameFilter;

import org.apache.calcite.util.Util;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.MaskingCallback;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.client.cli.utils.SqlScriptReader.parseSqlScript;
import static org.junit.Assert.assertEquals;

/** Test that runs every {@code xx.q} file in "resources/sql/" path as a test. */
@RunWith(Parameterized.class)
public class CliClientITCase extends AbstractTestBase {

    // a generated UDF jar used for testing classloading of dependencies
    private static URL udfDependency;
    private static Path historyPath;

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameter public String sqlPath;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() throws Exception {
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
        return paths.toArray();
    }

    @BeforeClass
    public static void setup() throws IOException {
        File udfJar =
                TestUserClassLoaderJar.createJarFile(
                        tempFolder.newFolder("test-jar"), "test-classloader-udf.jar");
        udfDependency = udfJar.toURI().toURL();
        historyPath = tempFolder.newFile("history").toPath();
    }

    @Test
    public void testSqlStatements() throws IOException {
        URL url = CliClientITCase.class.getResource("/" + sqlPath);
        String in = IOUtils.toString(url, StandardCharsets.UTF_8);
        List<TestSqlStatement> testSqlStatements = parseSqlScript(in);
        List<String> sqlStatements =
                testSqlStatements.stream().map(s -> s.sql).collect(Collectors.toList());
        List<Result> actualResults = runSqlStatements(sqlStatements);
        String out = transformOutput(testSqlStatements, actualResults);
        String errorMsg = "SQL script " + sqlPath + " is not passed.";
        assertEquals(errorMsg, in, out);
    }

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param statements the SQL statements to run
     * @return the printed results on SQL Client
     */
    private List<Result> runSqlStatements(List<String> statements) throws IOException {
        final String sqlContent = String.join("\n", statements);
        DefaultContext defaultContext =
                new DefaultContext(
                        new Environment(),
                        Collections.singletonList(udfDependency),
                        new Configuration(miniClusterResource.getClientConfiguration()),
                        Collections.singletonList(new DefaultCLI()));
        final Executor executor = new LocalExecutor(defaultContext);
        InputStream inputStream = new ByteArrayInputStream(sqlContent.getBytes());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(256);
        String sessionId = executor.openSession("test-session");

        try (Terminal terminal = new DumbTerminal(inputStream, outputStream);
                CliClient client =
                        new CliClient(
                                terminal,
                                sessionId,
                                executor,
                                historyPath,
                                HideSqlStatement.INSTANCE)) {
            client.open();
            String output = new String(outputStream.toByteArray());
            return normalizeOutput(output);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------------------------

    private static final String PROMOTE = "Flink SQL> ";
    private static final String INFO_BEGIN = "\u001B[34;1m";
    private static final String INFO_END = "\u001B[0m";
    private static final String ERROR_BEGIN = "\u001B[31;1m";
    private static final String ERROR_END = "\u001B[0m";

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
                contentLines.add(line);
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
        if (containsTag(contentLines, INFO_BEGIN) && containsTag(contentLines, INFO_END)) {
            return new Result(stripTagsAndConcatLines(contentLines, INFO_BEGIN, INFO_END), "!info");
        } else if (containsTag(contentLines, ERROR_BEGIN) && containsTag(contentLines, ERROR_END)) {
            return new Result(
                    stripTagsAndConcatLines(contentLines, ERROR_BEGIN, ERROR_END), "!error");
        } else {
            return new Result(stripTagsAndConcatLines(contentLines), "!ok");
        }
    }

    private static boolean containsTag(List<String> contents, String tag) {
        for (String content : contents) {
            if (content.contains(tag)) {
                return true;
            }
        }
        return false;
    }

    private static String stripTagsAndConcatLines(List<String> lines, String... tags) {
        List<String> newLines = new ArrayList<>();
        for (String line : lines) {
            String newLine = line;
            for (String tag : tags) {
                newLine = newLine.replace(tag, "");
            }
            // there might be trailing white spaces,
            // we should remove them because we don't compare trailing white spaces
            newLines.add(StringUtils.stripEnd(newLine, " "));
        }
        return String.join("\n", newLines);
    }

    private static String transformOutput(
            List<TestSqlStatement> testSqlStatements, List<Result> results) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < testSqlStatements.size(); i++) {
            TestSqlStatement sqlScript = testSqlStatements.get(i);
            out.append(sqlScript.comment).append(sqlScript.sql);
            if (i < results.size()) {
                Result result = results.get(i);
                out.append(result.content).append(result.flag).append("\n");
            }
        }
        return out.toString();
    }

    private static final class Result {
        final String content;
        final String flag;

        private Result(String content, String flag) {
            this.flag = flag;
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
