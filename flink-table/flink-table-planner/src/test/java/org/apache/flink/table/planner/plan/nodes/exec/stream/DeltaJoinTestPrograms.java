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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link TableTestProgram} definitions for testing {@link StreamExecDeltaJoin}. */
public class DeltaJoinTestPrograms {

    static final String[] LEFT_TABLE_BASE_SCHEMA =
            new String[] {"a1 int", "a0 double", "a2 string"};

    static final String[] RIGHT_TABLE_BASE_SCHEMA =
            new String[] {"b0 double", "b2 string", "b1 int"};

    static final String[] SINK_TABLE_BASE_SCHEMA =
            Stream.concat(
                            Arrays.stream(LEFT_TABLE_BASE_SCHEMA),
                            Arrays.stream(RIGHT_TABLE_BASE_SCHEMA))
                    .toArray(String[]::new);

    static final Map<String, String> TABLE_BASE_OPTIONS =
            Map.of("async", "true", "sink-insert-only", "false");

    public static final TableTestProgram DELTA_JOIN_WITH_JOIN_KEY_EQUALS_INDEX =
            TableTestProgram.of(
                            "delta-join-with-join-key-equals-index",
                            "validates delta join with join key equals index")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupTableSource(
                            SourceTestStep.newBuilder("leftSrc")
                                    .addSchema(LEFT_TABLE_BASE_SCHEMA)
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .addIndex("a1")
                                    .producedBeforeRestore(
                                            Row.of(1, 1.0, "l-1-1"),
                                            Row.of(1, 1.0, "l-1-2"),
                                            Row.of(5, 5.0, "l-5-1"))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(
                                            Row.of(3, 3.0, "l-3-1"),
                                            Row.of(3, 3.0, "l-3-2"),
                                            Row.of(5, 5.0, "l-5-2"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("rightSrc")
                                    .addSchema(RIGHT_TABLE_BASE_SCHEMA)
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .addIndex("b1")
                                    .producedBeforeRestore(
                                            Row.of(5.0, "r-5-1", 5), Row.of(3.0, "r-3-1", 3))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(
                                            Row.of(3.0, "r-3-2", 3), Row.of(1.0, "r-1-1", 1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("snk")
                                    .addSchema(addPk2Schema(SINK_TABLE_BASE_SCHEMA, "a0", "b0"))
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .testMaterializedData()
                                    // deduplicate data by pk
                                    .deduplicatedFieldIndices(new int[] {1, 3})
                                    .consumedBeforeRestore(Row.of(5, 5.0, "l-5-1", 5.0, "r-5-1", 5))
                                    .consumedAfterRestore(
                                            Row.of(1, 1.0, "l-1-2", 1.0, "r-1-1", 1),
                                            Row.of(3, 3.0, "l-3-2", 3.0, "r-3-2", 3),
                                            Row.of(5, 5.0, "l-5-2", 5.0, "r-5-1", 5))
                                    .build())
                    .runSql(
                            "insert into snk "
                                    + "select * from leftSrc join rightSrc "
                                    + "on a1 = b1")
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_JOIN_KEY_CONTAINS_INDEX =
            TableTestProgram.of(
                            "delta-join-with-join-key-contains-index",
                            "validates delta join with join key contains index")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupTableSources(
                            DELTA_JOIN_WITH_JOIN_KEY_EQUALS_INDEX.getSetupSourceTestSteps())
                    .setupTableSinks(DELTA_JOIN_WITH_JOIN_KEY_EQUALS_INDEX.getSetupSinkTestSteps())
                    .runSql(
                            "insert into snk "
                                    + "select * from leftSrc join rightSrc "
                                    + "on a1 = b1 and a0 = b0")
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_NON_EQUIV_CONDITION =
            TableTestProgram.of(
                            "delta-join-with-with-non-equiv-condition",
                            "validates delta join with non equiv condition")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupTableSource(
                            SourceTestStep.newBuilder("leftSrc")
                                    .addSchema(LEFT_TABLE_BASE_SCHEMA)
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .addIndex("a1")
                                    .producedBeforeRestore(
                                            Row.of(1, 1.0, "Tim"),
                                            Row.of(1, 1.0, "Sandy"),
                                            Row.of(5, 5.0, "Bob"))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(
                                            Row.of(3, 3.0, "Lilith"),
                                            Row.of(3, 3.1, "Lilith"),
                                            Row.of(5, 5.0, "Jim"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("rightSrc")
                                    .addSchema(RIGHT_TABLE_BASE_SCHEMA)
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .addIndex("b1")
                                    .producedBeforeRestore(
                                            Row.of(5.0, "Mark", 5), Row.of(3.0, "Lilith", 3))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(
                                            Row.of(3.2, "Lilith", 3), Row.of(1.0, "Tim", 1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("snk")
                                    .addSchema(addPk2Schema(SINK_TABLE_BASE_SCHEMA, "a1", "b1"))
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .testMaterializedData()
                                    // deduplicate data by pk
                                    .deduplicatedFieldIndices(new int[] {0, 5})
                                    .consumedBeforeRestore(Row.of(5, 5.0, "Bob", 5.0, "Mark", 5))
                                    .consumedAfterRestore(
                                            Row.of(1, 1.0, "Sandy", 1.0, "Tim", 1),
                                            Row.of(5, 5.0, "Jim", 5.0, "Mark", 5))
                                    .build())
                    .runSql(
                            "insert into snk "
                                    + "select * from leftSrc join rightSrc "
                                    + "on a1 = b1 and a2 <> b2")
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_CALC_ON_SOURCE =
            TableTestProgram.of(
                            "delta-join-with-calc-on-source",
                            "validates delta join with calc on source")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupTableSources(
                            DELTA_JOIN_WITH_JOIN_KEY_EQUALS_INDEX.getSetupSourceTestSteps())
                    .setupTableSink(
                            SinkTestStep.newBuilder("snk")
                                    .addSchema(addPk2Schema(SINK_TABLE_BASE_SCHEMA, "a0", "b0"))
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .testMaterializedData()
                                    // deduplicate data by pk
                                    .deduplicatedFieldIndices(new int[] {1, 3})
                                    .consumedBeforeRestore(Row.of(6, 5.0, "l-5-1", 5.0, "r-5-1", 6))
                                    .consumedAfterRestore(Row.of(6, 5.0, "l-5-2", 5.0, "r-5-1", 6))
                                    .build())
                    .runSql(
                            "insert into snk "
                                    + "select new_a1, a0, a2, b0, b2, new_b1 from ( "
                                    + "   select a0, a1, a2, a1 + 1 as new_a1 from leftSrc "
                                    + "       where a1 = 1 or a1 = 5 "
                                    + ") join ("
                                    + "   select b0, b1, b1 + 1 as new_b1, b2 from rightSrc "
                                    + "       where b0 = cast(3.0 as double) or b0 = cast(5.0 as double) "
                                    + ") "
                                    + "on a1 = b1 and a0 = b0")
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_CALC_ON_SOURCE_AND_FILTER_PUSHED_DOWN =
            TableTestProgram.of(
                            "delta-join-with-calc-on-source-and-filter-pushed-down",
                            "validates delta join with calc on source and filter pushed down")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED, true)
                    .setupTableSources(
                            DELTA_JOIN_WITH_CALC_ON_SOURCE.getSetupSourceTestSteps().stream()
                                    .map(
                                            sourceTestStep -> {
                                                String filterableFields;
                                                if (sourceTestStep.name.equals("leftSrc")) {
                                                    filterableFields = "a1";
                                                } else if (sourceTestStep.name.equals("rightSrc")) {
                                                    filterableFields = "b0";
                                                } else {
                                                    throw new IllegalStateException(
                                                            "Unknown test table name: "
                                                                    + sourceTestStep.name);
                                                }
                                                Map<String, String> oldOptions =
                                                        new HashMap<>(sourceTestStep.options);
                                                oldOptions.put(
                                                        "filterable-fields", filterableFields);
                                                return sourceTestStep.withNewOptions(oldOptions);
                                            })
                                    .collect(Collectors.toList()))
                    .setupTableSinks(DELTA_JOIN_WITH_CALC_ON_SOURCE.getSetupSinkTestSteps())
                    .runSql(DELTA_JOIN_WITH_CALC_ON_SOURCE.getRunSqlTestStep().sql)
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_CACHE =
            TableTestProgram.of("delta-join-with-cache", "validates delta join with cache")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED, true)
                    .setupTableSources(
                            DELTA_JOIN_WITH_NON_EQUIV_CONDITION.getSetupSourceTestSteps())
                    .setupTableSinks(DELTA_JOIN_WITH_NON_EQUIV_CONDITION.getSetupSinkTestSteps())
                    .runSql(DELTA_JOIN_WITH_NON_EQUIV_CONDITION.getRunSqlTestStep().sql)
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_CACHE_AND_CALC_ON_SOURCE =
            TableTestProgram.of(
                            "delta-join-with-cache-and-calc-on-source",
                            "validates delta join with cache and calc on source")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED, true)
                    .setupTableSources(DELTA_JOIN_WITH_CALC_ON_SOURCE.getSetupSourceTestSteps())
                    .setupTableSinks(DELTA_JOIN_WITH_CALC_ON_SOURCE.getSetupSinkTestSteps())
                    .runSql(DELTA_JOIN_WITH_CALC_ON_SOURCE.getRunSqlTestStep().sql)
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_CDC_SOURCE_WITHOUT_DELETE =
            TableTestProgram.of(
                            "delta-join-with-cdc-source-without-delete",
                            "validates delta join with cdc source without delete")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupTableSource(
                            SourceTestStep.newBuilder("leftSrc")
                                    .addSchema(addPk2Schema(LEFT_TABLE_BASE_SCHEMA, "a1", "a0"))
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .addOption("changelog-mode", "I,UA,UB")
                                    .addIndex("a1")
                                    .producedBeforeRestore(
                                            // insert pk1
                                            Row.ofKind(RowKind.INSERT, 1, 1.0, "left-pk1-1"),
                                            // update pk1
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1.0, "left-pk1-1"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, 1.0, "left-pk1-2"),
                                            // insert pk2
                                            Row.ofKind(RowKind.INSERT, 1, 2.0, "left-pk2-1"))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(
                                            // update pk2
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, 2.0, "left-pk2-1"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, 2.0, "left-pk2-2"),
                                            // insert pk3
                                            Row.ofKind(RowKind.INSERT, 3, 3.0, "left-pk3"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("rightSrc")
                                    .addSchema(addPk2Schema(RIGHT_TABLE_BASE_SCHEMA, "b0", "b1"))
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .addOption("changelog-mode", "I,UA,UB")
                                    .addIndex("b1")
                                    .producedBeforeRestore(
                                            // insert pk1
                                            Row.ofKind(RowKind.INSERT, 2.0, "right-pk1-1", 1),
                                            // insert pk2
                                            Row.ofKind(RowKind.INSERT, 1.0, "right-pk2", 1))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(
                                            // update pk1
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, 2.0, "right-pk1-1", 1),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2.0, "right-pk1-2", 1),
                                            // insert pk3
                                            Row.ofKind(RowKind.INSERT, 3.0, "right-pk3-1", 3),
                                            // update pk3
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, 3.0, "right-pk3-1", 3),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 3.0, "right-pk3-2", 3))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("snk")
                                    .addSchema(
                                            addPk2Schema(
                                                    SINK_TABLE_BASE_SCHEMA, "a0", "b0", "a1", "b1"))
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .testMaterializedData()
                                    // deduplicate data by pk
                                    .deduplicatedFieldIndices(new int[] {0, 1, 3, 5})
                                    .consumedBeforeRestore(
                                            Row.of(1, 1.0, "left-pk1-2", 2.0, "right-pk1-1", 1),
                                            Row.of(1, 2.0, "left-pk2-1", 2.0, "right-pk1-1", 1),
                                            Row.of(1, 1.0, "left-pk1-2", 1.0, "right-pk2", 1),
                                            Row.of(1, 2.0, "left-pk2-1", 1.0, "right-pk2", 1))
                                    .consumedAfterRestore(
                                            Row.of(1, 1.0, "left-pk1-2", 2.0, "right-pk1-2", 1),
                                            Row.of(1, 2.0, "left-pk2-2", 2.0, "right-pk1-2", 1),
                                            Row.of(1, 2.0, "left-pk2-2", 1.0, "right-pk2", 1),
                                            Row.of(3, 3.0, "left-pk3", 3.0, "right-pk3-2", 3))
                                    .build())
                    .runSql(
                            "insert into snk "
                                    + "select * from leftSrc join rightSrc "
                                    + "on a1 = b1")
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_CALC_ON_CDC_SOURCE_WITHOUT_DELETE =
            TableTestProgram.of(
                            "delta-join-with-calc-on-cdc-source-without-delete",
                            "validates delta join with calc on cdc source without delete ")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED, true)
                    .setupTableSources(
                            DELTA_JOIN_WITH_CDC_SOURCE_WITHOUT_DELETE.getSetupSourceTestSteps())
                    .setupTableSink(
                            SinkTestStep.newBuilder("snk")
                                    .addSchema(
                                            addPk2Schema(
                                                    SINK_TABLE_BASE_SCHEMA, "a0", "b0", "a1", "b1"))
                                    .addOptions(TABLE_BASE_OPTIONS)
                                    .testMaterializedData()
                                    // deduplicate data by pk
                                    .deduplicatedFieldIndices(new int[] {0, 1, 3, 5})
                                    .consumedBeforeRestore(
                                            Row.of(1, 1.0, "left-pk1-2-s", 2.0, "right-pk1-1-s", 1),
                                            Row.of(1, 2.0, "left-pk2-1-s", 2.0, "right-pk1-1-s", 1))
                                    .consumedAfterRestore(
                                            Row.of(1, 1.0, "left-pk1-2-s", 2.0, "right-pk1-2-s", 1),
                                            Row.of(1, 2.0, "left-pk2-2-s", 2.0, "right-pk1-2-s", 1))
                                    .build())
                    .runSql(
                            "insert into snk "
                                    + "select a1, a0, new_a2, b0, new_b2, b1 from ( "
                                    + "   select a0, a1, a2, concat(a2, '-s') as new_a2 from leftSrc "
                                    + "       where a0 = cast(1.0 as double) or a0 = cast(2.0 as double) "
                                    + ") join ("
                                    + "   select b0, b1, concat(b2, '-s') as new_b2, b2 from rightSrc "
                                    + "       where b0 = cast(2.0 as double) or b0 = cast(3.0 as double) "
                                    + ") "
                                    + "on a1 = b1")
                    .build();

    public static final TableTestProgram DELTA_JOIN_WITH_CACHE_AND_CDC_SOURCE_WITHOUT_DELETE =
            TableTestProgram.of(
                            "delta-join-with-cache-and-cdc-source-without-delete",
                            "validates delta join with cache and cdc source without delete")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                            OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED, true)
                    .setupTableSources(
                            DELTA_JOIN_WITH_CDC_SOURCE_WITHOUT_DELETE.getSetupSourceTestSteps())
                    .setupTableSinks(
                            DELTA_JOIN_WITH_CDC_SOURCE_WITHOUT_DELETE.getSetupSinkTestSteps())
                    .runSql(DELTA_JOIN_WITH_CDC_SOURCE_WITHOUT_DELETE.getRunSqlTestStep().sql)
                    .build();

    public static final TableTestProgram
            DELTA_JOIN_WITH_CACHE_AND_CALC_ON_CDC_SOURCE_WITHOUT_DELETE =
                    TableTestProgram.of(
                                    "delta-join-with-cache-and-calc-on-cdc-source-without-delete",
                                    "validates delta join with cache and calc on cdc source without delete")
                            .setupConfig(
                                    OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                                    OptimizerConfigOptions.DeltaJoinStrategy.FORCE)
                            .setupConfig(
                                    ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED,
                                    true)
                            .setupTableSources(
                                    DELTA_JOIN_WITH_CALC_ON_CDC_SOURCE_WITHOUT_DELETE
                                            .getSetupSourceTestSteps())
                            .setupTableSinks(
                                    DELTA_JOIN_WITH_CALC_ON_CDC_SOURCE_WITHOUT_DELETE
                                            .getSetupSinkTestSteps())
                            .runSql(
                                    DELTA_JOIN_WITH_CALC_ON_CDC_SOURCE_WITHOUT_DELETE
                                            .getRunSqlTestStep()
                                            .sql)
                            .build();

    private static String[] addPk2Schema(String[] originalSchema, String... pkCols) {
        return Stream.concat(
                        Arrays.stream(originalSchema),
                        Stream.of(
                                Arrays.stream(pkCols)
                                        .collect(
                                                Collectors.joining(
                                                        ", ", "primary key (", ") not enforced"))))
                .toArray(String[]::new);
    }
}
