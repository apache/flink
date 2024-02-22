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

/** {@link TableTestProgram} definitions for testing {@link StreamExecIntervalJoin}. */
public class IntervalJoinTestPrograms {

    static final Row[] ORDER_BEFORE_DATA = {
        Row.of(1, "2020-04-15 08:00:01"),
        Row.of(2, "2020-04-15 08:00:02"),
        Row.of(4, "2020-04-15 08:00:04"),
        Row.of(5, "2020-04-15 08:00:05"),
        Row.of(3, "2020-04-15 08:00:03")
    };

    static final Row[] SHIPMENT_BEFORE_DATA = {
        Row.of(2, 1, "2020-04-15 08:00:02"),
        Row.of(5, 2, "2020-04-15 08:00:05"),
        Row.of(6, 5, "2020-04-15 08:00:06"),
        Row.of(15, 4, "2020-04-15 08:00:15"),
        Row.of(16, 6, "2020-04-15 08:00:15")
    };

    static final Row[] ORDER_AFTER_DATA = {
        Row.of(7, "2020-04-15 08:00:09"), Row.of(10, "2020-04-15 08:00:11"),
    };

    static final Row[] SHIPMENT_AFTER_DATA = {
        Row.of(7, 3, "2020-04-15 08:00:15"),
        Row.of(11, 7, "2020-04-15 08:00:16"),
        Row.of(13, 10, "2020-04-15 08:00:16")
    };

    static final String[] ORDERS_EVENT_TIME_SCHEMA = {
        "id INT",
        "order_ts_str STRING",
        "order_ts AS TO_TIMESTAMP(order_ts_str)",
        "WATERMARK for `order_ts` AS `order_ts` - INTERVAL '1' SECOND"
    };

    static final String[] ORDERS_PROC_TIME_SCHEMA = {
        "id INT", "order_ts_str STRING", "proc_time AS PROCTIME()"
    };

    static final String[] SHIPMENTS_EVENT_TIME_SCHEMA = {
        "id INT",
        "order_id INT",
        "shipment_ts_str STRING",
        "shipment_ts AS TO_TIMESTAMP(shipment_ts_str)",
        "WATERMARK for `shipment_ts` AS `shipment_ts` - INTERVAL '1' SECOND"
    };

    static final String[] SHIPMENTS_PROC_TIME_SCHEMA = {
        "id INT", "order_id INT", "shipment_ts_str STRING", "`proc_time` AS PROCTIME()"
    };

    static final String[] SINK_SCHEMA = {
        "order_id INT", "order_ts_str STRING", "shipment_ts_str STRING"
    };

    static final TableTestProgram INTERVAL_JOIN_EVENT_TIME =
            TableTestProgram.of(
                            "interval-join-event-time", "validates interval join using event time")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders_t")
                                    .addSchema(ORDERS_EVENT_TIME_SCHEMA)
                                    .producedBeforeRestore(ORDER_BEFORE_DATA)
                                    .producedAfterRestore(ORDER_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("shipments_t")
                                    .addSchema(SHIPMENTS_EVENT_TIME_SCHEMA)
                                    .producedBeforeRestore(SHIPMENT_BEFORE_DATA)
                                    .producedAfterRestore(SHIPMENT_AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 2020-04-15 08:00:01, 2020-04-15 08:00:02]",
                                            "+I[2, 2020-04-15 08:00:02, 2020-04-15 08:00:05]",
                                            "+I[5, 2020-04-15 08:00:05, 2020-04-15 08:00:06]")
                                    .consumedAfterRestore(
                                            "+I[10, 2020-04-15 08:00:11, 2020-04-15 08:00:16]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT\n"
                                    + "     o.id AS order_id,\n"
                                    + "     o.order_ts_str,\n"
                                    + "     s.shipment_ts_str\n"
                                    + " FROM orders_t o\n"
                                    + " JOIN shipments_t s ON o.id = s.order_id\n"
                                    + " WHERE o.order_ts BETWEEN s.shipment_ts - INTERVAL '5' SECOND AND s.shipment_ts + INTERVAL '5' SECOND;")
                    .build();

    static final TableTestProgram INTERVAL_JOIN_PROC_TIME =
            TableTestProgram.of(
                            "interval-join-proc-time",
                            "validates interval join using processing time")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders_t")
                                    .addSchema(ORDERS_PROC_TIME_SCHEMA)
                                    .producedBeforeRestore(ORDER_BEFORE_DATA)
                                    .producedAfterRestore(ORDER_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("shipments_t")
                                    .addSchema(SHIPMENTS_PROC_TIME_SCHEMA)
                                    .producedBeforeRestore(SHIPMENT_BEFORE_DATA)
                                    .producedAfterRestore(SHIPMENT_AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 2020-04-15 08:00:01, 2020-04-15 08:00:02]",
                                            "+I[2, 2020-04-15 08:00:02, 2020-04-15 08:00:05]",
                                            "+I[5, 2020-04-15 08:00:05, 2020-04-15 08:00:06]",
                                            "+I[4, 2020-04-15 08:00:04, 2020-04-15 08:00:15]")
                                    .consumedAfterRestore(
                                            "+I[7, 2020-04-15 08:00:09, 2020-04-15 08:00:16]",
                                            "+I[10, 2020-04-15 08:00:11, 2020-04-15 08:00:16]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT\n"
                                    + "     o.id AS order_id,\n"
                                    + "     o.order_ts_str,\n"
                                    + "     s.shipment_ts_str\n"
                                    + " FROM orders_t o\n"
                                    + " JOIN shipments_t s ON o.id = s.order_id\n"
                                    + " WHERE o.proc_time BETWEEN s.proc_time - INTERVAL '5' SECOND AND s.proc_time + INTERVAL '5' SECOND;")
                    .build();

    static final TableTestProgram INTERVAL_JOIN_NEGATIVE_INTERVAL =
            TableTestProgram.of(
                            "interval-join-negative-interval",
                            "validates interval join using event time")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders_t")
                                    .addSchema(ORDERS_EVENT_TIME_SCHEMA)
                                    .producedBeforeRestore(ORDER_BEFORE_DATA)
                                    .producedAfterRestore(ORDER_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("shipments_t")
                                    .addSchema(SHIPMENTS_EVENT_TIME_SCHEMA)
                                    .producedBeforeRestore(SHIPMENT_BEFORE_DATA)
                                    .producedAfterRestore(SHIPMENT_AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[1, 2020-04-15 08:00:01, null]",
                                            "+I[2, 2020-04-15 08:00:02, null]",
                                            "+I[4, 2020-04-15 08:00:04, null]",
                                            "+I[5, 2020-04-15 08:00:05, null]",
                                            "+I[3, 2020-04-15 08:00:03, null]")
                                    .consumedAfterRestore(
                                            "+I[7, 2020-04-15 08:00:09, null]",
                                            "+I[10, 2020-04-15 08:00:11, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT\n"
                                    + "     o.id AS order_id,\n"
                                    + "     o.order_ts_str,\n"
                                    + "     s.shipment_ts_str\n"
                                    + " FROM orders_t o LEFT OUTER JOIN shipments_t s\n"
                                    + " ON o.id = s.order_id\n"
                                    + " AND o.order_ts BETWEEN s.shipment_ts + INTERVAL '10' SECOND AND s.shipment_ts + INTERVAL '5' SECOND;")
                    .build();
}
