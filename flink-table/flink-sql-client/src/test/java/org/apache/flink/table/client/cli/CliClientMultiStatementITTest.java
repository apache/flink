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

import org.apache.flink.table.client.cli.utils.SqlScriptReader;
import org.apache.flink.table.client.cli.utils.TestSqlStatement;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.flink.shaded.guava31.com.google.common.io.PatternFilenameFilter;

import org.apache.calcite.util.Util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Test that runs every {@code xx.q} file in "resources/sql_multi/" path as a test. */
class CliClientMultiStatementITTest extends CliClientITCase {

    static Stream<String> sqlPaths() throws Exception {
        String first = "sql_multi/statement_set.q";
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

    protected List<TestSqlStatement> parseSqlScript(String input) {
        return SqlScriptReader.parseMultiStatementSqlScript(input);
    }

    protected String transformOutput(
            List<TestSqlStatement> testSqlStatements, List<Result> results) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < testSqlStatements.size(); i++) {
            TestSqlStatement sqlScript = testSqlStatements.get(i);
            out.append(sqlScript.comment)
                    .append(sqlScript.sql)
                    .append(SqlScriptReader.HINT_START_OF_OUTPUT)
                    .append("\n");
            if (i < results.size()) {
                Result result = results.get(i);
                String content =
                        TableTestUtil.replaceNodeIdInOperator(removeExecNodeId(result.content));
                out.append(content).append(result.highestTag.tag).append("\n");
            }
        }

        return out.toString();
    }
}
