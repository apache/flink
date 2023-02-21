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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Simple example for demonstrating the use of SQL in Java.
 *
 * <p>Usage: {@code ./bin/flink run ./examples/table/StreamWindowSQLExample.jar}
 *
 * <p>This example shows how to: - Register a table via DDL - Declare an event time attribute in the
 * DDL - Run a streaming window aggregate on the registered table
 */
public class StreamWindowSQLExample {

    public static void main(String[] args) throws Exception {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,beer,3,2019-12-12 00:00:01\n"
                        + "1,diaper,4,2019-12-12 00:00:02\n"
                        + "2,pen,3,2019-12-12 00:00:04\n"
                        + "2,rubber,3,2019-12-12 00:00:06\n"
                        + "3,rubber,2,2019-12-12 00:00:05\n"
                        + "4,beer,1,2019-12-12 00:00:08";
        String path = createTempFile(contents);

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE orders (\n"
                        + "  user_id INT,\n"
                        + "  product STRING,\n"
                        + "  amount INT,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(ddl);

        // run a SQL query on the table and retrieve the result as a new Table
        String query =
                "SELECT\n"
                        + "  CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start,\n"
                        + "  COUNT(*) order_num,\n"
                        + "  SUM(amount) total_amount,\n"
                        + "  COUNT(DISTINCT product) unique_products\n"
                        + "FROM orders\n"
                        + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        tEnv.executeSql(query).print();
        // should output:
        // +----+--------------------------------+--------------+--------------+-----------------+
        // | op |                   window_start |    order_num | total_amount | unique_products |
        // +----+--------------------------------+--------------+--------------+-----------------+
        // | +I |        2019-12-12 00:00:00.000 |            3 |           10 |               3 |
        // | +I |        2019-12-12 00:00:05.000 |            3 |            6 |               2 |
        // +----+--------------------------------+--------------+--------------+-----------------+

    }

    /** Creates a temporary file with the contents and returns the absolute path. */
    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("orders", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }
}
