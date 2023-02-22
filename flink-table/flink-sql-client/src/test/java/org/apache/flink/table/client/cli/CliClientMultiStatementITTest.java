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
import org.apache.flink.table.gateway.utils.TestSqlStatement;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import java.util.List;
import java.util.stream.Collectors;

/** Test that runs every {@code xx.q} file in "resources/sql_multi/" path as a test. */
public class CliClientMultiStatementITTest extends CliClientITCase {

    @Parameters(name = "parameters={0}")
    public static List<TestParameters> parameters() throws Exception {
        return listTestSpecInTheSameModule("sql_multi").stream()
                .map(TestParameters::new)
                .collect(Collectors.toList());
    }

    @Override
    protected List<TestSqlStatement> parseSqlStatements(String input) {
        return SqlScriptReader.parseMultiStatementSqlScript(input);
    }

    @Override
    protected String transformOutput(List<Result> results) {
        StringBuilder out = new StringBuilder();
        for (Result result : results) {
            String content =
                    TableTestUtil.replaceNodeIdInOperator(removeExecNodeId(result.content));
            out.append(SqlScriptReader.HINT_START_OF_OUTPUT)
                    .append("\n")
                    .append(content)
                    .append(result.highestTag.tag)
                    .append("\n");
        }

        return out.toString();
    }
}
