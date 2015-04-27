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
 * {@link org.apache.flink.api.java.table.TableEnvironment} can be used to create a
 * {@link org.apache.flink.api.table.Table} from a {@link org.apache.flink.api.java.DataSet}
 * or {@link org.apache.flink.streaming.api.datastream.DataStream}.
 *
 * <p>
 * This can be used to perform SQL-like queries on data. Please have
 * a look at {@link org.apache.flink.api.table.Table} to see which operations are supported and
 * how query strings are written.
 *
 * <p>
 * Example:
 *
 * <code>
 * ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
 *
 * DataSet<WC> input = env.fromElements(
 *   new WC("Hello", 1),
 *   new WC("Ciao", 1),
 *   new WC("Hello", 1));
 *
 * Table table = TableUtil.from(input);
 *
 * Table filtered = table
 *     .groupBy("word")
 *     .select("word.count as count, word")
 *     .filter("count = 2");
 *
 * DataSet<WC> result = TableUtil.toSet(filtered, WC.class);
 *
 * result.print();
 * env.execute();
 * </code>
 *
 * <p>
 * As seen above, a {@link org.apache.flink.api.table.Table} can be converted back to the
 * underlying API representation using {@link org.apache.flink.api.java.table.TableEnvironment.toSet}
 * or {@link org.apache.flink.api.java.table.TableEnvironment.toStream}.
 */
package org.apache.flink.api.java.table;
