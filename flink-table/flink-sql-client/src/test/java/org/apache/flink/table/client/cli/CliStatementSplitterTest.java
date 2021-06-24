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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test {@link CliStatementSplitter}. */
public class CliStatementSplitterTest {

    @Test
    public void testIsNotEndOfStatement() {
        // TODO: fix ';
        List<String> lines = Arrays.asList(" --;", "", "select -- ok;", "\n");
        for (String line : lines) {
            assertFalse(
                    String.format("%s is not end of statement but get true", line),
                    CliStatementSplitter.isStatementComplete(line));
        }
    }

    @Test
    public void testIsEndOfStatement() {
        List<String> lines = Arrays.asList(" ; --;", "select a from b;-- ok;", ";\n");
        for (String line : lines) {
            assertTrue(
                    String.format("%s is end of statement but get false", line),
                    CliStatementSplitter.isStatementComplete(line));
        }
    }

    @Test
    public void testSplitContent() {
        List<String> lines =
                Arrays.asList(
                        "-- Define Table; \n"
                                + "CREATE TABLE MyTable (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'test-property' = 'test.value'\n);"
                                + "-- Define Table;",
                        "SET a = b;",
                        "\n" + "SELECT func(id) from MyTable\n;");
        List<String> actual = CliStatementSplitter.splitContent(String.join("\n", lines));

        List<String> expected =
                Arrays.asList(
                        "\nCREATE TABLE MyTable (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'test-property' = 'test.value'\n);"
                                + "-- Define Table;",
                        "SET a = b;",
                        "\n" + "SELECT func(id) from MyTable\n;");

        for (int i = 0; i < lines.size(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
