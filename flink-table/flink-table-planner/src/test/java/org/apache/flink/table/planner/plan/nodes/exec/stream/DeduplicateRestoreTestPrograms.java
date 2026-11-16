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

/** {@link TableTestProgram} definitions for restore tests of the DEDUPLICATE PTF. */
public class DeduplicateRestoreTestPrograms {

    public static final TableTestProgram FIRST_ARRIVAL_RESTORE =
            TableTestProgram.of(
                            "deduplicate-first-arrival-restore",
                            "with the default keep => 'first', a key emitted before the restore "
                                    + "stays dropped after it, and a key first seen after it is "
                                    + "inserted")
                    .setupTableSource(
                            SourceTestStep.newBuilder("payments")
                                    .addSchema("payment_id STRING", "amount INT")
                                    .producedBeforeRestore(
                                            Row.of("p1", 50),
                                            Row.of("p2", 20),
                                            Row.of("p1", 60),
                                            Row.of("p3", 30))
                                    .producedAfterRestore(
                                            Row.of("p1", 70),
                                            Row.of("p2", 20),
                                            Row.of("p4", 40),
                                            Row.of("p3", 35))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "payment_id STRING PRIMARY KEY NOT ENFORCED",
                                            "amount INT")
                                    .addOption("sink-changelog-mode-enforced", "I,UA,D")
                                    .consumedBeforeRestore("+I[p1, 50]", "+I[p2, 20]", "+I[p3, 30]")
                                    .consumedAfterRestore("+I[p4, 40]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE payments PARTITION BY payment_id)")
                    .build();

    public static final TableTestProgram SEQUENCE_BY_RESTORE =
            TableTestProgram.of(
                            "deduplicate-sequence-by-restore",
                            "the sequence_by value of the kept record survives a restore, so "
                                    + "with 'first' a greater or equal value arriving after the "
                                    + "restore is dropped")
                    .setupTableSource(
                            SourceTestStep.newBuilder("customer_changes")
                                    .addSchema("customer_id STRING", "city STRING", "lsn BIGINT")
                                    .producedBeforeRestore(
                                            Row.of("c1", "Berlin", 100L),
                                            Row.of("c2", "Paris", 105L),
                                            Row.of("c1", "Hamburg", 90L),
                                            Row.of("c3", "Rome", 50L))
                                    .producedAfterRestore(
                                            Row.of("c1", "Munich", 95L),
                                            Row.of("c2", "Lyon", 100L),
                                            Row.of("c3", "Milan", 50L),
                                            Row.of("c4", "Oslo", 1L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "customer_id STRING PRIMARY KEY NOT ENFORCED",
                                            "city STRING",
                                            "lsn BIGINT")
                                    .addOption("sink-changelog-mode-enforced", "I,UA,D")
                                    .consumedBeforeRestore(
                                            "+I[c1, Berlin, 100]",
                                            "+I[c2, Paris, 105]",
                                            "+U[c1, Hamburg, 90]",
                                            "+I[c3, Rome, 50]")
                                    .consumedAfterRestore("+U[c2, Lyon, 100]", "+I[c4, Oslo, 1]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE customer_changes PARTITION BY customer_id, "
                                    + "keep => 'first', "
                                    + "sequence_by => DESCRIPTOR(lsn))")
                    .build();
}
