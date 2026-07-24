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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** The famous word count example that shows a minimal Flink SQL job in batch execution mode. */
public final class WordCountSQLExample {

    public static void main(String[] args) throws Exception {

        // set up the Table API
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        // execute a Flink SQL job and print the result locally
        tableEnv.executeSql(
                        // define the aggregation
                        "SELECT word, SUM(frequency) AS `count`\n"
                                // read from an artificial fixed-size table with rows and columns
                                + "FROM (\n"
                                + "  VALUES ('Hello', 1), ('Ciao', 1), ('Hello', 2)\n"
                                + ")\n"
                                // name the table and its columns
                                + "AS WordTable(word, frequency)\n"
                                // group for aggregation
                                + "GROUP BY word")
                .print();
    }
}
