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

import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.util.Arrays;

/** {@link TableTestProgram} definitions for testing {@link StreamExecDeduplicate}. */
public class TableSinkTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of(1, 1L, "hi"), Row.of(2, 2L, "hello"), Row.of(3, 2L, "hello world")
    };

    static final Row[] AFTER_DATA = {Row.of(4, 4L, "foo"), Row.of(5, 2L, "foo bar")};

    static final String[] SOURCE_SCHEMA = {"a INT", "b BIGINT", "c VARCHAR"};

    static final TableTestProgram SINK_BUCKETING_WITH_COUNT =
            buildBucketingTest("with-count", TableDistribution.ofUnknown(3));
    static final TableTestProgram SINK_BUCKETING_WITH_KEYS_AND_COUNT =
            buildBucketingTest(
                    "with-keys-and-count", TableDistribution.ofUnknown(Arrays.asList("a"), 3));
    static final TableTestProgram SINK_BUCKETING_HASH_WITH_KEYS_AND_COUNT =
            buildBucketingTest(
                    "hash-with-keys-with-count", TableDistribution.ofHash(Arrays.asList("a"), 3));
    static final TableTestProgram SINK_BUCKETING_HASH_WITH_KEYS_AND_WITHOUT_COUNT =
            buildBucketingTest(
                    "range_with_keys_without_count",
                    TableDistribution.ofHash(Arrays.asList("a"), null));

    private static TableTestProgram buildBucketingTest(
            final String suffix, final TableDistribution distribution) {
        return TableTestProgram.of("sink-bucketing_" + suffix, "validates sink bucketing")
                .setupTableSource(
                        SourceTestStep.newBuilder("source_t")
                                .addSchema(SOURCE_SCHEMA)
                                .producedBeforeRestore(BEFORE_DATA)
                                .producedAfterRestore(AFTER_DATA)
                                .build())
                .setupTableSink(
                        SinkTestStep.newBuilder("sink_t")
                                .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                .addDistribution(distribution)
                                .addPartitionKeys("b")
                                .consumedBeforeRestore(
                                        "+I[1, 1, hi]", "+I[2, 2, hello]", "+I[3, 2, hello world]")
                                .consumedAfterRestore("+I[4, 4, foo]", "+I[5, 2, foo bar]")
                                .build())
                .runSql("INSERT INTO sink_t SELECT * FROM source_t")
                .build();
    }

    static final TableTestProgram SINK_PARTITION =
            TableTestProgram.of("sink-partition", "validates sink partition")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "a INT", "b BIGINT", "p BIGINT NOT NULL", "c VARCHAR")
                                    .addPartitionKeys("b")
                                    .addOption("partition-list", "b:1;b:2;b:3;b:4")
                                    .consumedBeforeRestore(
                                            "+I[1, 2, 1, hi]",
                                            "+I[2, 2, 2, hello]",
                                            "+I[3, 2, 2, hello world]")
                                    .consumedAfterRestore(
                                            "+I[4, 2, 4, foo]", "+I[5, 2, 2, foo bar]")
                                    .build())
                    .runSql("INSERT INTO sink_t PARTITION (b=2) SELECT * FROM source_t")
                    .build();

    static final TableTestProgram SINK_OVERWRITE =
            TableTestProgram.of("sink-overwrite", "validates sink with overwrite")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, hi]",
                                            "+I[2, 2, hello]",
                                            "+I[3, 2, hello world]")
                                    .consumedAfterRestore("+I[4, 4, foo]", "+I[5, 2, foo bar]")
                                    .build())
                    .runSql("INSERT OVERWRITE sink_t SELECT * FROM source_t")
                    .build();
    static final TableTestProgram SINK_WRITING_METADATA =
            TableTestProgram.of("sink-writing-metadata", "validates writing metadata to sink")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR METADATA")
                                    .addOption("writable-metadata", "c:STRING")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, hi]",
                                            "+I[2, 2, hello]",
                                            "+I[3, 2, hello world]")
                                    .consumedAfterRestore("+I[4, 4, foo]", "+I[5, 2, foo bar]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t")
                    .build();

    static final TableTestProgram SINK_NDF_PRIMARY_KEY =
            TableTestProgram.of(
                            "sink-ndf-primary-key",
                            "validates sink with ndf and different primary key")
                    .setupTemporaryCatalogFunction(
                            "ndf", JavaUserDefinedScalarFunctions.NonDeterministicUdf.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "c VARCHAR",
                                            "PRIMARY KEY(c) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, hi--1170105035]",
                                            "+I[2, 2, hello-234785527]",
                                            "+I[3, 2, hello world--1360544799]")
                                    .consumedAfterRestore(
                                            "+I[4, 4, foo--1170105035]",
                                            "+I[5, 2, foo bar-234785527]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, ndf(c) FROM source_t")
                    .build();

    static final TableTestProgram SINK_PARTIAL_INSERT =
            TableTestProgram.of("sink-partial-insert", "validates sink with partial insert")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "c VARCHAR",
                                            "d DECIMAL(10,2)",
                                            "e DOUBLE")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, hi, null, null]",
                                            "+I[2, 2, hello, null, null]",
                                            "+I[3, 2, hello world, null, null]")
                                    .consumedAfterRestore(
                                            "+I[4, 4, foo, null, null]",
                                            "+I[5, 2, foo bar, null, null]")
                                    .build())
                    .runSql("INSERT INTO sink_t (a, b, c) SELECT a, b, c FROM source_t")
                    .build();
}
