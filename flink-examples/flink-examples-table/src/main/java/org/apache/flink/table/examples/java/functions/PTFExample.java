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

package org.apache.flink.table.examples.java.functions;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.flink.table.annotation.ArgumentTrait.SCALAR;

public class PTFExample {

    public static void main(String[] args) throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY FUNCTION file_reader AS '%s'",
                        PTFFunction.class.getName()));
        String filePath =
                Preconditions.checkNotNull(
                                PTFExample.class.getClassLoader().getResource("test.file"))
                        .getPath();
        // Simply print the data
        tEnv.executeSql(String.format("SELECT * FROM file_reader('%s')", filePath)).print();
        // Join with another table
        tEnv.executeSql(
                        String.format(
                                "SELECT * FROM ("
                                        + "VALUES (1), (2), (3)"
                                        + ")"
                                        + "LEFT JOIN (SELECT content FROM file_reader('%s')) ON TRUE",
                                filePath))
                .print();
        // Join PTF directly is not supported.
        //        tEnv.executeSql(
        //                        String.format(
        //                                "SELECT * FROM ("
        //                                        + "VALUES (1), (2), (3)"
        //                                        + ")"
        //                                        + "LEFT JOIN file_reader('%s') ON TRUE",
        //                                filePath))
        //                .print();
    }

    @FunctionHint(output = @DataTypeHint("ROW<content STRING>"))
    public static class PTFFunction extends ProcessTableFunction<Row> {

        private static final long serialVersionUID = 1L;

        public void eval(@ArgumentHint(SCALAR) String path) {
            try {
                List<String> lines = Files.readAllLines(Paths.get(path));
                for (String line : lines) {
                    collect(Row.of(line));
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read files.", e);
            }
        }
    }
}
