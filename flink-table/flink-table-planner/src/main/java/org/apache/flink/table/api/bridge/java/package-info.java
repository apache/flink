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

/**
 * <strong>Table API (Java)</strong><br>
 *
 * A {@link org.apache.flink.table.api.bridge.java.BatchTableEnvironment} can be used to create a
 * {@link org.apache.flink.table.api.Table} from a {@link org.apache.flink.api.java.DataSet}.
 * Equivalently, a {@link org.apache.flink.table.api.bridge.java.StreamTableEnvironment} can be used to
 * create a {@link org.apache.flink.table.api.Table} from a
 * {@link org.apache.flink.streaming.api.datastream.DataStream}.
 *
 * <p>
 * Tables can be used to perform SQL-like queries on data. Please have
 * a look at {@link org.apache.flink.table.api.Table} to see which operations are supported and
 * how query strings are written.
 *
 * <p>
 * Example:
 *
 * <pre>{@code
 * ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 * BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
 *
 * DataSet<WC> input = env.fromElements(
 *   new WC("Hello", 1),
 *   new WC("Ciao", 1),
 *   new WC("Hello", 1));
 *
 * Table table = tEnv.fromDataSet(input);
 *
 * Table filtered = table
 *     .groupBy("word")
 *     .select("word.count as count, word")
 *     .filter("count = 2");
 *
 * DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);
 *
 * result.print();
 * }</pre>
 *
 * <p>
 * As seen above, a {@link org.apache.flink.table.api.Table} can be converted back to the
 * underlying API representation using
 * {@link org.apache.flink.table.api.bridge.java.BatchTableEnvironment#toDataSet(Table, java.lang.Class)},
 * {@link org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toAppendStream(Table, java.lang.Class)}}, or
 * {@link org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toRetractStream(Table, java.lang.Class)}}.
 */

package org.apache.flink.table.api.bridge.java;

import org.apache.flink.table.api.Table;
