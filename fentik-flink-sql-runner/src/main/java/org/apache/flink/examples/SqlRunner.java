/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fentik;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputDeserializer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Main class for executing SQL scripts. */
public class SqlRunner {
    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";

    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

    private static void readFully(InputStream in, byte[] buffer) throws IOException {
        int pos = 0;
        int remaining = buffer.length;

        while (remaining > 0) {
            int read = in.read(buffer, pos, remaining);
            if (read == -1) {
                return;
            }
            pos += read;
            remaining -= read;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new Exception("Expected SQL query file or s3 path.");
        }
        var path = new Path(args[0]);
        var fs = path.getFileSystem();
        var in = fs.open(path);
        byte[] buffer = new byte[1024*1024];
        readFully(in, buffer);

        var script = new String(buffer);
        var statements = parseStatements(script);

        Configuration configuration = new Configuration();
        for (String statement : statements) {
            if (statement.indexOf("SET ") == 0) {
                // Example: SET parallelism.default='64';
                String[] set = statement.split(" ")[1].split("=");
                String key = set[0].replace("'", "");
                String value = set[1].replace("'", "").replace(";", "").replace("\n", "");
                configuration.setString(key, value);
            }
        }
        var tableEnv = TableEnvironment.create(configuration);
        for (String statement : statements) {
            if (statement.indexOf("SET ") != 0) {
                tableEnv.executeSql(statement);
            }
        }
    }

    public static List<String> parseStatements(String script) {
        var formatted = formatSqlFile(script).replaceAll(COMMENT_PATTERN, "");
        var statements = new ArrayList<String>();

        StringBuilder current = null;
        boolean statementSet = false;
        for (String line : formatted.split("\n")) {
            var trimmed = line.trim();
            if (trimmed.isBlank()) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }
            if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
                statementSet = true;
            }
            current.append(trimmed);
            current.append("\n");
            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                if (!statementSet || trimmed.equals("END;")) {
                    statements.add(current.toString());
                    current = null;
                    statementSet = false;
                }
            }
        }
        return statements;
    }

    public static String formatSqlFile(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }
}
