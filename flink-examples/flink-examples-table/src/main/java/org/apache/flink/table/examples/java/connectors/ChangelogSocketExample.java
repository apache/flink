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

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;

/**
 * Example for implementing a custom {@link DynamicTableSource} and a {@link DecodingFormat}.
 *
 * <p>The example implements a table source with a decoding format that supports changelog
 * semantics.
 *
 * <p>The {@link SocketDynamicTableFactory} illustrates how connector components play together. It
 * can serve as a reference implementation for implementing own connectors and/or formats.
 *
 * <p>The {@link SocketDynamicTableSource} uses a simple single-threaded {@link SourceFunction} to
 * open a socket that listens for incoming bytes. The raw bytes are decoded into rows by a pluggable
 * format. The format expects a changelog flag as the first column.
 *
 * <p>In particular, the example shows how to
 *
 * <ul>
 *   <li>create factories that parse and validate options,
 *   <li>implement table connectors,
 *   <li>implement and discover custom formats,
 *   <li>and use provided utilities such as data structure converters and the {@link FactoryUtil}.
 * </ul>
 *
 * <p>Usage: <code>ChangelogSocketExample --hostname &lt;localhost&gt; --port &lt;9999&gt;</code>
 *
 * <p>Use the following command to ingest data in a terminal:
 *
 * <pre>
 *     nc -lk 9999
 *     INSERT|Alice|12
 *     INSERT|Bob|5
 *     DELETE|Alice|12
 *     INSERT|Alice|18
 * </pre>
 *
 * <p>The result is written to stdout.
 */
public final class ChangelogSocketExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostname = params.get("hostname", "localhost");
        final String port = params.get("port", "9999");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // source only supports parallelism of 1

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register a table in the catalog
        tEnv.executeSql(
                "CREATE TABLE UserScores (name STRING, score INT)\n"
                        + "WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = '"
                        + hostname
                        + "',\n"
                        + "  'port' = '"
                        + port
                        + "',\n"
                        + "  'byte-delimiter' = '10',\n"
                        + "  'format' = 'changelog-csv',\n"
                        + "  'changelog-csv.column-delimiter' = '|'\n"
                        + ")");

        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("SELECT name, SUM(score) FROM UserScores GROUP BY name");

        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}
