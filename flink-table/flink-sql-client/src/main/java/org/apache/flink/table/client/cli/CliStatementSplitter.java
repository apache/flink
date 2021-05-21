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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Line splitter to determine whether the submitted line is complete. It also offers to split the
 * submitted content into multiple statements.
 *
 * <p>This is a simple splitter. It just split the line in context-unrelated way, e.g. it fails to
 * parse line "';\n'"
 */
public class CliStatementSplitter {

    private static final String MASK = "--.*$";
    private static final String BEGINNING_MASK = "^(\\s)*--.*$";

    public static boolean isStatementComplete(String statement) {
        String[] lines = statement.split("\n");
        // fix input statement is "\n"
        if (lines.length == 0) {
            return false;
        } else {
            return isEndOfStatement(lines[lines.length - 1]);
        }
    }

    public static List<String> splitContent(String content) {
        List<String> statements = new ArrayList<>();
        List<String> buffer = new ArrayList<>();

        for (String line : content.split("\n")) {
            if (isEndOfStatement(line)) {
                buffer.add(line);
                statements.add(normalize(buffer));
                buffer.clear();
            } else {
                buffer.add(line);
            }
        }
        if (!buffer.isEmpty()) {
            statements.add(normalize(buffer));
        }
        return statements;
    }

    private static String normalize(List<String> buffer) {
        // remove comment lines
        return buffer.stream()
                .map(statementLine -> statementLine.replaceAll(BEGINNING_MASK, ""))
                .collect(Collectors.joining("\n"));
    }

    private static boolean isEndOfStatement(String line) {
        return line.replaceAll(MASK, "").trim().endsWith(";");
    }
}
