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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;

import static org.apache.flink.table.planner.plan.nodes.exec.common.LookupJoinTestPrograms.CUSTOMERS_BEFORE_DATA;
import static org.apache.flink.table.planner.plan.nodes.exec.common.LookupJoinTestPrograms.CUSTOMERS_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.common.LookupJoinTestPrograms.ORDERS_BEFORE_DATA;
import static org.apache.flink.table.planner.plan.nodes.exec.common.LookupJoinTestPrograms.ORDERS_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.common.LookupJoinTestPrograms.SINK_SCHEMA;

/** {@link TableTestProgram} definitions for semantic testing {@link StreamExecLookupJoin}. */
public class LookupJoinSemanticTestPrograms {

    /** A semantic test cannot take a source that carries after-restore data. */
    static final SourceTestStep CUSTOMERS =
            SourceTestStep.newBuilder("customers_t")
                    .addOption("disable-lookup", "false") // static/lookup table
                    .addOption("filterable-fields", "age")
                    .addSchema(CUSTOMERS_SCHEMA)
                    .producedValues(CUSTOMERS_BEFORE_DATA)
                    .build();

    static final SourceTestStep ORDERS =
            SourceTestStep.newBuilder("orders_t")
                    .addOption("filterable-fields", "customer_id")
                    .addSchema(ORDERS_SCHEMA)
                    .producedValues(ORDERS_BEFORE_DATA)
                    .build();

    private static String filteredLookupJoin(String filter) {
        return "SELECT "
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
                + "ON O.customer_id = C.id AND "
                + filter;
    }

    /**
     * Both branches select the same columns from the same dim table and differ only in the filter
     * pushed into it, so the two lookup joins are indistinguishable unless the pushed-down filter
     * is part of the lookup join's digest. See FLINK-36808.
     */
    private static String unionOfTwoFilteredLookupJoins(String firstFilter, String secondFilter) {
        return "INSERT INTO sink_t "
                + filteredLookupJoin(firstFilter)
                + " UNION ALL "
                + filteredLookupJoin(secondFilter);
    }

    private static SinkTestStep sink() {
        return SinkTestStep.newBuilder("sink_t")
                .addSchema(SINK_SCHEMA)
                .consumedValues(
                        "+I[1, 44.44, 3, Claire, 37, Austin, Texas, 73301]",
                        "+I[2, 100.02, 5, Jake, 42, New York City, New York, 10001]",
                        "+I[4, 92.61, 2, Alice, 32, San Francisco, California, 95016]",
                        "+I[5, 12.78, 2, Alice, 32, San Francisco, California, 95016]")
                .build();
    }

    public static final TableTestProgram LOOKUP_JOIN_UNION_DIFFERENT_FILTERS =
            TableTestProgram.of(
                            "lookup-join-union-different-filters",
                            "validates two lookup joins on the same table with different pushed-down filters are not merged, with the matching filter first")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(sink())
                    .runSql(unionOfTwoFilteredLookupJoins("C.age > 30", "C.age > 100"))
                    .build();

    public static final TableTestProgram LOOKUP_JOIN_UNION_DIFFERENT_FILTERS_REVERSED =
            TableTestProgram.of(
                            "lookup-join-union-different-filters-reversed",
                            "validates the same with the non-matching filter first, where a wrong merge returns nothing at all")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(sink())
                    .runSql(unionOfTwoFilteredLookupJoins("C.age > 100", "C.age > 30"))
                    .build();
}
