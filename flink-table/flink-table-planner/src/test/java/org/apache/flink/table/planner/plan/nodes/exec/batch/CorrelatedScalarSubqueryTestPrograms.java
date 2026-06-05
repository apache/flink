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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.math.BigDecimal;

/**
 * {@link TableTestProgram} definitions for decorrelation of a correlated scalar sub-query whose
 * FROM is a join (the TPC-H Q2 shape). The correlated predicate sits above the join inside the
 * aggregated sub-query, a shape the decorrelator must not leave a {@code $cor0} in.
 */
public class CorrelatedScalarSubqueryTestPrograms {

    public static final TableTestProgram SCALAR_SUBQUERY_OVER_JOIN =
            TableTestProgram.of(
                            "correlated-scalar-subquery-over-join",
                            "correlated scalar aggregate sub-query whose FROM is a join")
                    .setupTableSource(
                            SourceTestStep.newBuilder("part")
                                    .addSchema("p_partkey BIGINT")
                                    .producedValues(Row.of(5L), Row.of(10L))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("partsupp")
                                    .addSchema(
                                            "ps_partkey BIGINT",
                                            "ps_suppkey BIGINT",
                                            "ps_supplycost DECIMAL(10, 2)")
                                    .producedValues(
                                            Row.of(5L, 100L, new BigDecimal("5.00")),
                                            Row.of(10L, 100L, new BigDecimal("7.00")))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("supplier")
                                    .addSchema("s_suppkey BIGINT")
                                    .producedValues(Row.of(100L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("p_partkey BIGINT")
                                    .consumedValues("+I[5]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT p_partkey FROM part\n"
                                    + "WHERE p_partkey = (\n"
                                    + "  SELECT min(ps_supplycost) FROM partsupp, supplier\n"
                                    + "  WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey)")
                    .build();
}
