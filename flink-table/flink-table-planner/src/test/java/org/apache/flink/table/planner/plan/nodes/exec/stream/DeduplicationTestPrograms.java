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
import org.apache.flink.types.RowKind;

/** {@link TableTestProgram} definitions for testing {@link StreamExecDeduplicate}. */
public class DeduplicationTestPrograms {

    static final Row[] DATA1 = {
        Row.of(1L, "terry", "pen", 1000L),
        Row.of(2L, "alice", "pen", 2000L),
        Row.of(3L, "bob", "pen", 3000L),
        Row.of(4L, "bob", "apple", 4000L),
        Row.of(5L, "fish", "apple", 5000L)
    };

    static final Row[] DATA2 = {
        Row.of(6L, "jerry", "pen", 6000L),
        Row.of(7L, "larry", "apple", 7000L),
        Row.of(8L, "bill", "banana", 8000L),
        Row.of(9L, "carol", "apple", 9000L)
    };

    static final TableTestProgram DEDUPLICATE =
            TableTestProgram.of("deduplicate-asc", "validates deduplication in ascending")
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable")
                                    .addSchema(
                                            "order_id bigint",
                                            "`user` varchar",
                                            "product varchar",
                                            "order_time bigint ",
                                            "event_time as TO_TIMESTAMP(FROM_UNIXTIME(order_time)) ",
                                            "watermark for event_time as event_time - INTERVAL '5' second ")
                                    .producedBeforeRestore(DATA1)
                                    .producedAfterRestore(DATA2)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema(
                                            "order_id bigint",
                                            "`user` varchar",
                                            "product varchar",
                                            "order_time bigint",
                                            "primary key(product) not enforced")
                                    .consumedBeforeRestore(
                                            Row.of(1, "terry", "pen", 1000),
                                            Row.of(4, "bob", "apple", 4000))
                                    .consumedAfterRestore(Row.of(8L, "bill", "banana", 8000L))
                                    .build())
                    .runSql(
                            "insert into MySink "
                                    + "select order_id, user, product, order_time \n"
                                    + "FROM ("
                                    + "  SELECT *,"
                                    + "    ROW_NUMBER() OVER (PARTITION BY product ORDER BY event_time ASC) AS row_num\n"
                                    + "  FROM MyTable)"
                                    + "WHERE row_num = 1")
                    .build();

    static final TableTestProgram DEDUPLICATE_PROCTIME =
            TableTestProgram.of(
                            "deduplicate-asc-proctime",
                            "validates deduplication in ascending with proctime")
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable")
                                    .addSchema(
                                            "order_id bigint",
                                            "`user` varchar",
                                            "product varchar",
                                            "order_time bigint ",
                                            "event_time as TO_TIMESTAMP(FROM_UNIXTIME(order_time)) ",
                                            "proctime AS PROCTIME() ")
                                    .producedBeforeRestore(DATA1)
                                    .producedAfterRestore(DATA2)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema(
                                            "order_id bigint",
                                            "`user` varchar",
                                            "product varchar",
                                            "order_time bigint",
                                            "primary key(product) not enforced")
                                    .consumedBeforeRestore(
                                            Row.of(1, "terry", "pen", 1000),
                                            Row.of(4, "bob", "apple", 4000))
                                    .consumedAfterRestore(Row.of(8L, "bill", "banana", 8000L))
                                    .build())
                    .runSql(
                            "insert into MySink "
                                    + "select order_id, user, product, order_time \n"
                                    + "FROM ("
                                    + "  SELECT *,"
                                    + "    ROW_NUMBER() OVER (PARTITION BY product ORDER BY proctime ASC) AS row_num\n"
                                    + "  FROM MyTable)"
                                    + "WHERE row_num = 1")
                    .build();

    static final TableTestProgram DEDUPLICATE_DESC =
            TableTestProgram.of("deduplicate-desc", "validates deduplication in descending")
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable")
                                    .addSchema(
                                            "order_id bigint",
                                            "`user` varchar",
                                            "product varchar",
                                            "order_time bigint ",
                                            "event_time as TO_TIMESTAMP(FROM_UNIXTIME(order_time)) ",
                                            "watermark for event_time as event_time - INTERVAL '5' second ")
                                    .producedBeforeRestore(DATA1)
                                    .producedAfterRestore(DATA2)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema(
                                            "order_id bigint",
                                            "`user` varchar",
                                            "product varchar",
                                            "order_time bigint",
                                            "primary key(product) not enforced")
                                    .consumedBeforeRestore(
                                            Row.of(1L, "terry", "pen", 1000L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    2L,
                                                    "alice",
                                                    "pen",
                                                    2000L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER, 3L, "bob", "pen", 3000L),
                                            Row.of(4L, "bob", "apple", 4000L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    5L,
                                                    "fish",
                                                    "apple",
                                                    5000L))
                                    .consumedAfterRestore(
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    6L,
                                                    "jerry",
                                                    "pen",
                                                    6000L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    7L,
                                                    "larry",
                                                    "apple",
                                                    7000L),
                                            Row.of(8L, "bill", "banana", 8000L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    9L,
                                                    "carol",
                                                    "apple",
                                                    9000L))
                                    .build())
                    .runSql(
                            "insert into MySink "
                                    + "select order_id, user, product, order_time \n"
                                    + "FROM ("
                                    + "  SELECT *,"
                                    + "    ROW_NUMBER() OVER (PARTITION BY product ORDER BY event_time DESC) AS row_num\n"
                                    + "  FROM MyTable)"
                                    + "WHERE row_num = 1")
                    .build();
}
