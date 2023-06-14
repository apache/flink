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

package org.apache.flink.table.jdbc.driver.tests;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** E2E tests for flink jdbc driver. */
public class FlinkDriverExample {

    public static void main(String[] args) throws Exception {
        final String driver = "org.apache.flink.table.jdbc.FlinkDriver";
        final String url = "jdbc:flink://localhost:8083";
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String tableDir = args[0];
        String tableOutput = String.format("%s/output.dat", tableDir);
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement statement = connection.createStatement()) {
                checkState(
                        !statement.execute(
                                String.format(
                                        "CREATE TABLE test_table(id bigint, val int, str string) "
                                                + "with ("
                                                + "'connector'='filesystem',\n"
                                                + "'format'='csv',\n"
                                                + "'path'='%s/test_table')",
                                        tableDir)));
                // INSERT TABLE returns job id
                checkState(
                        statement.execute(
                                "INSERT INTO test_table VALUES "
                                        + "(1, 11, '111'), "
                                        + "(3, 33, '333'), "
                                        + "(2, 22, '222'), "
                                        + "(4, 44, '444')"));
                String jobId;
                try (ResultSet resultSet = statement.getResultSet()) {
                    checkState(resultSet.next());
                    checkState(resultSet.getMetaData().getColumnCount() == 1);
                    jobId = resultSet.getString("job id");
                    checkState(!resultSet.next());
                }
                boolean jobFinished = false;
                while (!jobFinished) {
                    checkState(statement.execute("SHOW JOBS"));
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            if (resultSet.getString(1).equals(jobId)) {
                                if (resultSet.getString(3).equals("FINISHED")) {
                                    jobFinished = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                // SELECT all data from test_table
                List<String> resultList = new ArrayList<>();
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test_table")) {
                    while (resultSet.next()) {
                        resultList.add(
                                String.format(
                                        "%s,%s,%s",
                                        resultSet.getLong("id"),
                                        resultSet.getInt("val"),
                                        resultSet.getString("str")));
                    }
                }
                Collections.sort(resultList);
                BufferedWriter bw = new BufferedWriter(new FileWriter(tableOutput));
                for (String result : resultList) {
                    bw.write(result);
                    bw.newLine();
                }
                bw.flush();
                bw.close();
            }
        }
    }
}
