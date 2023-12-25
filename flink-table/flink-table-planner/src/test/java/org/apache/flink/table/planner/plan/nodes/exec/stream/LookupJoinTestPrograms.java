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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** {@link TableTestProgram} definitions for testing {@link StreamExecLookupJoin}. */
public class LookupJoinTestPrograms {

    static final String[] CUSTOMERS_SCHEMA =
            new String[] {
                "id INT PRIMARY KEY NOT ENFORCED",
                "name STRING",
                "age INT",
                "city STRING",
                "state STRING",
                "zipcode INT"
            };

    static final Row[] CUSTOMERS_BEFORE_DATA =
            new Row[] {
                Row.of(1, "Bob", 28, "Mountain View", "California", 94043),
                Row.of(2, "Alice", 32, "San Francisco", "California", 95016),
                Row.of(3, "Claire", 37, "Austin", "Texas", 73301),
                Row.of(4, "Shannon", 29, "Boise", "Idaho", 83701),
                Row.of(5, "Jake", 42, "New York City", "New York", 10001)
            };

    static final Row[] CUSTOMERS_AFTER_DATA =
            new Row[] {
                Row.of(1, "Bob", 28, "San Jose", "California", 94089),
                Row.of(6, "Joana", 54, "Atlanta", "Georgia", 30033)
            };

    static final SourceTestStep CUSTOMERS =
            SourceTestStep.newBuilder("customers_t")
                    .addOption("disable-lookup", "false") // static/lookup table
                    .addOption("filterable-fields", "age")
                    .addSchema(CUSTOMERS_SCHEMA)
                    // Note: The before/after data is used to initialize the lookup table
                    // The data is not consumed like regular tables since lookup tables are
                    // external tables with data already present in them.
                    // Therefore, no state is persisted for lookup tables
                    .producedBeforeRestore(CUSTOMERS_BEFORE_DATA)
                    .producedAfterRestore(CUSTOMERS_AFTER_DATA)
                    .build();

    static final SourceTestStep CUSTOMERS_ASYNC =
            SourceTestStep.newBuilder("customers_t")
                    .addOption("disable-lookup", "false") // static/lookup table
                    .addOption("filterable-fields", "age")
                    .addOption("async", "true")
                    .addSchema(CUSTOMERS_SCHEMA)
                    // Note: The before/after data is used to initialize the lookup table
                    // The data is not consumed like regular tables since lookup tables are
                    // external tables with data already present in them.
                    // Therefore, no state is persisted for lookup tables
                    .producedBeforeRestore(CUSTOMERS_BEFORE_DATA)
                    .producedAfterRestore(CUSTOMERS_AFTER_DATA)
                    .build();

    static final SourceTestStep ORDERS =
            SourceTestStep.newBuilder("orders_t")
                    .addOption("filterable-fields", "customer_id")
                    .addSchema(
                            "order_id INT",
                            "customer_id INT",
                            "total DOUBLE",
                            "order_time STRING",
                            "proc_time AS PROCTIME()")
                    .producedBeforeRestore(
                            Row.of(1, 3, 44.44, "2020-10-10 00:00:01"),
                            Row.of(2, 5, 100.02, "2020-10-10 00:00:02"),
                            Row.of(4, 2, 92.61, "2020-10-10 00:00:04"),
                            Row.of(3, 1, 23.89, "2020-10-10 00:00:03"),
                            Row.of(6, 4, 7.65, "2020-10-10 00:00:06"),
                            Row.of(5, 2, 12.78, "2020-10-10 00:00:05"))
                    .producedAfterRestore(
                            Row.of(7, 6, 17.58, "2020-10-10 00:00:07"), // new customer
                            Row.of(9, 1, 143.21, "2020-10-10 00:00:08") // updated zip code
                            )
                    .build();

    static final List<String> SINK_SCHEMA =
            Arrays.asList(
                    "order_id INT",
                    "total DOUBLE",
                    "id INT",
                    "name STRING",
                    "age INT",
                    "city STRING",
                    "state STRING",
                    "zipcode INT");

    static final TableTestProgram LOOKUP_JOIN_PROJECT_PUSHDOWN =
            TableTestProgram.of(
                            "lookup-join-project-pushdown",
                            "validates lookup join with project pushdown")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            SINK_SCHEMA.stream()
                                                    .filter(field -> !field.equals("age INT"))
                                                    .collect(Collectors.toList()))
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, 3, Claire, Austin, Texas, 73301]",
                                            "+I[2, 100.02, 5, Jake, New York City, New York, 10001]",
                                            "+I[4, 92.61, 2, Alice, San Francisco, California, 95016]",
                                            "+I[3, 23.89, 1, Bob, Mountain View, California, 94043]",
                                            "+I[6, 7.65, 4, Shannon, Boise, Idaho, 83701]",
                                            "+I[5, 12.78, 2, Alice, San Francisco, California, 95016]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, 1, Bob, San Jose, California, 94089]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id")
                    .build();

    static final TableTestProgram LOOKUP_JOIN_FILTER_PUSHDOWN =
            TableTestProgram.of(
                            "lookup-join-filter-pushdown",
                            "validates lookup join with filter pushdown")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, 3, Claire, 37, Austin, Texas, 73301]",
                                            "+I[2, 100.02, 5, Jake, 42, New York City, New York, 10001]",
                                            "+I[4, 92.61, 2, Alice, 32, San Francisco, California, 95016]",
                                            "+I[5, 12.78, 2, Alice, 32, San Francisco, California, 95016]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, 54, Atlanta, Georgia, 30033]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.age, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id AND C.age > 30")
                    .build();

    static final TableTestProgram LOOKUP_JOIN_PRE_FILTER =
            TableTestProgram.of("lookup-join-pre-filter", "validates lookup join with pre filter")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, 3, Claire, 37, Austin, Texas, 73301]",
                                            "+I[2, 100.02, 5, Jake, 42, New York City, New York, 10001]",
                                            "+I[4, 92.61, 2, Alice, 32, San Francisco, California, 95016]",
                                            "+I[3, 23.89, null, null, null, null, null, null]",
                                            "+I[6, 7.65, null, null, null, null, null, null]",
                                            "+I[5, 12.78, null, null, null, null, null, null]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, 54, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, null, null, null, null, null, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.age, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "LEFT JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id AND C.age > 30 AND O.total > 15.3")
                    .build();

    static final TableTestProgram LOOKUP_JOIN_POST_FILTER =
            TableTestProgram.of("lookup-join-post-filter", "validates lookup join with post filter")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, null, null, null, null, null, null]",
                                            "+I[2, 100.02, null, null, null, null, null, null]",
                                            "+I[4, 92.61, null, null, null, null, null, null]",
                                            "+I[3, 23.89, 1, Bob, 28, Mountain View, California, 94043]",
                                            "+I[6, 7.65, 4, Shannon, 29, Boise, Idaho, 83701]",
                                            "+I[5, 12.78, 2, Alice, 32, San Francisco, California, 95016]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, 54, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, null, null, null, null, null, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.age, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "LEFT JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id AND CAST(O.total AS INT) < C.age")
                    .build();

    static final TableTestProgram LOOKUP_JOIN_PRE_POST_FILTER =
            TableTestProgram.of(
                            "lookup-join-pre-post-filter",
                            "validates lookup join with pre and post filters")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, null, null, null, null, null, null]",
                                            "+I[2, 100.02, null, null, null, null, null, null]",
                                            "+I[4, 92.61, null, null, null, null, null, null]",
                                            "+I[3, 23.89, 1, Bob, 28, Mountain View, California, 94043]",
                                            "+I[6, 7.65, null, null, null, null, null, null]",
                                            "+I[5, 12.78, null, null, null, null, null, null]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, 54, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, null, null, null, null, null, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.age, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "LEFT JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id AND O.total > 15.3 AND CAST(O.total AS INT) < C.age")
                    .build();

    static final TableTestProgram LOOKUP_JOIN_LEFT_JOIN =
            TableTestProgram.of("lookup-join-left-join", "validates lookup join with left join")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, null, null, null, null, null, null]",
                                            "+I[2, 100.02, 5, Jake, 42, New York City, New York, 10001]",
                                            "+I[4, 92.61, 2, Alice, 32, San Francisco, California, 95016]",
                                            "+I[3, 23.89, null, null, null, null, null, null]",
                                            "+I[6, 7.65, null, null, null, null, null, null]",
                                            "+I[5, 12.78, 2, Alice, 32, San Francisco, California, 95016]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, 54, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, null, null, null, null, null, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.age, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "LEFT JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id AND C.age > 30 AND O.customer_id <> 3")
                    .build();

    static final TableTestProgram LOOKUP_JOIN_ASYNC_HINT =
            TableTestProgram.of("lookup-join-async-hint", "validates lookup join with async hint")
                    .setupTableSource(CUSTOMERS_ASYNC)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, 3, Claire, 37, Austin, Texas, 73301]",
                                            "+I[2, 100.02, 5, Jake, 42, New York City, New York, 10001]",
                                            "+I[4, 92.61, 2, Alice, 32, San Francisco, California, 95016]",
                                            "+I[3, 23.89, 1, Bob, 28, Mountain View, California, 94043]",
                                            "+I[6, 7.65, 4, Shannon, 29, Boise, Idaho, 83701]",
                                            "+I[5, 12.78, 2, Alice, 32, San Francisco, California, 95016]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, 54, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, 1, Bob, 28, San Jose, California, 94089]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "/*+ LOOKUP('table'='C', 'async'='true', 'output-mode'='allow_unordered', 'timeout'='500s', 'capacity'='2000') */ "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.age, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id")
                    .build();

    static final TableTestProgram LOOKUP_JOIN_RETRY_HINT =
            TableTestProgram.of("lookup-join-retry-hint", "validates lookup join with retry hint")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, 3, Claire, 37, Austin, Texas, 73301]",
                                            "+I[2, 100.02, 5, Jake, 42, New York City, New York, 10001]",
                                            "+I[4, 92.61, 2, Alice, 32, San Francisco, California, 95016]",
                                            "+I[3, 23.89, 1, Bob, 28, Mountain View, California, 94043]",
                                            "+I[6, 7.65, 4, Shannon, 29, Boise, Idaho, 83701]",
                                            "+I[5, 12.78, 2, Alice, 32, San Francisco, California, 95016]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, 6, Joana, 54, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, 1, Bob, 28, San Jose, California, 94089]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "/*+ LOOKUP('table'='C', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ "
                                    + "O.order_id, "
                                    + "O.total, "
                                    + "C.id, "
                                    + "C.name, "
                                    + "C.age, "
                                    + "C.city, "
                                    + "C.state, "
                                    + "C.zipcode "
                                    + "FROM orders_t as O "
                                    + "JOIN customers_t FOR SYSTEM_TIME AS OF O.proc_time AS C "
                                    + "ON O.customer_id = C.id")
                    .build();
}
