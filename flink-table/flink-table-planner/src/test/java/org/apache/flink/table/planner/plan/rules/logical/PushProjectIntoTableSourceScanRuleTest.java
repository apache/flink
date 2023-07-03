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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PushProjectIntoTableSourceScanRule}. */
public class PushProjectIntoTableSourceScanRuleTest
        extends PushProjectIntoLegacyTableSourceScanRuleTest {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Override
    public void setup() {
        util().buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
        CalciteConfig calciteConfig =
                TableConfigUtils.getCalciteConfig(util().tableEnv().getConfig());
        calciteConfig
                .getBatchProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(RuleSets.ofList(PushProjectIntoTableSourceScanRule.INSTANCE))
                                .build());

        String ddl1 =
                "CREATE TABLE MyTable (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl1);

        String ddl2 =
                "CREATE TABLE VirtualTable (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  d as a + 1\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl2);

        String ddl3 =
                "CREATE TABLE NestedTable (\n"
                        + "  id int,\n"
                        + "  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n"
                        + "  nested row<name string, `value` int>,\n"
                        + "  `deepNestedWith.` row<`.value` int, nested row<name string, `.value` int>>,\n"
                        + "  name string,\n"
                        + "  testMap Map<string, string>\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'nested-projection-supported' = 'true',"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl3);

        String ddl4 =
                "CREATE TABLE MetadataTable(\n"
                        + "  id int,\n"
                        + "  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n"
                        + "  metadata_1 int metadata,\n"
                        + "  metadata_2 string metadata,\n"
                        + "  metadata_3 as cast(metadata_1 as bigint)\n"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'nested-projection-supported' = 'true',"
                        + " 'bounded' = 'true',\n"
                        + " 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'"
                        + ")";
        util().tableEnv().executeSql(ddl4);

        String ddl5 =
                "CREATE TABLE UpsertTable("
                        + "  id int,\n"
                        + "  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n"
                        + "  metadata_1 int metadata,\n"
                        + "  metadata_2 string metadata,\n"
                        + "  PRIMARY KEY(id, deepNested) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'nested-projection-supported' = 'true',"
                        + "  'bounded' = 'false',\n"
                        + "  'changelod-mode' = 'I,UB,D',"
                        + " 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'"
                        + ")";
        util().tableEnv().executeSql(ddl5);

        String ddl6 =
                "CREATE TABLE NestedItemTable (\n"
                        + "  `ID` INT,\n"
                        + "  `Timestamp` TIMESTAMP(3),\n"
                        + "  `Result` ROW<\n"
                        + "    `Mid` ROW<"
                        + "      `data_arr` ROW<`value` BIGINT> ARRAY,\n"
                        + "      `data_map` MAP<STRING, ROW<`value` BIGINT>>"
                        + "     >"
                        + "   >,\n"
                        + "   WATERMARK FOR `Timestamp` AS `Timestamp`\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'nested-projection-supported' = 'true',"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl6);

        String ddl7 =
                "CREATE TABLE ItemTable (\n"
                        + "  `ID` INT,\n"
                        + "  `Timestamp` TIMESTAMP(3),\n"
                        + "  `Result` ROW<\n"
                        + "    `data_arr` ROW<`value` BIGINT> ARRAY,\n"
                        + "    `data_map` MAP<STRING, ROW<`value` BIGINT>>>,\n"
                        + "  `outer_array` ARRAY<INT>,\n"
                        + "  `outer_map` MAP<STRING, STRING>,\n"
                        + "   WATERMARK FOR `Timestamp` AS `Timestamp`\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl7);
    }

    @Test
    public void testProjectWithMapType() {
        String sqlQuery = "SELECT id, testMap['e']\n" + "FROM NestedTable";
        util().verifyRelPlan(sqlQuery);
    }

    @Override
    @Test
    public void testNestedProject() {
        String sqlQuery =
                "SELECT id,\n"
                        + "    deepNested.nested1.name AS nestedName,\n"
                        + "    nested.`value` AS nestedValue,\n"
                        + "    deepNested.nested2.flag AS nestedFlag,\n"
                        + "    deepNested.nested2.num AS nestedNum\n"
                        + "FROM NestedTable";
        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testComplicatedNestedProject() {
        String sqlQuery =
                "SELECT id,"
                        + "    deepNested.nested1.name AS nestedName,\n"
                        + "    (`deepNestedWith.`.`.value` + `deepNestedWith.`.nested.`.value`) AS nestedSum\n"
                        + "FROM NestedTable";
        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testProjectWithDuplicateMetadataKey() {
        String sqlQuery = "SELECT id, metadata_3, metadata_1 FROM MetadataTable";

        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testNestProjectWithMetadata() {
        String sqlQuery =
                "SELECT id,"
                        + "    deepNested.nested1 AS nested1,\n"
                        + "    deepNested.nested1.`value` + deepNested.nested2.num + metadata_1 as results\n"
                        + "FROM MetadataTable";

        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testNestProjectWithUpsertSource() {
        String sqlQuery =
                "SELECT id,"
                        + "    deepNested.nested1 AS nested1,\n"
                        + "    deepNested.nested1.`value` + deepNested.nested2.num + metadata_1 as results\n"
                        + "FROM MetadataTable";

        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testNestedProjectFieldAccessWithITEM() {
        util().verifyRelPlan(
                        "SELECT "
                                + "`Result`.`Mid`.data_arr[ID].`value`, "
                                + "`Result`.`Mid`.data_map['item'].`value` "
                                + "FROM NestedItemTable");
    }

    @Test
    public void testNestedProjectFieldAccessWithITEMWithConstantIndex() {
        util().verifyRelPlan(
                        "SELECT "
                                + "`Result`.`Mid`.data_arr[2].`value`, "
                                + "`Result`.`Mid`.data_arr "
                                + "FROM NestedItemTable");
    }

    @Test
    public void testNestedProjectFieldAccessWithITEMContainsTopLevelAccess() {
        util().verifyRelPlan(
                        "SELECT "
                                + "`Result`.`Mid`.data_arr[2].`value`, "
                                + "`Result`.`Mid`.data_arr[ID].`value`, "
                                + "`Result`.`Mid`.data_map['item'].`value`, "
                                + "`Result`.`Mid` "
                                + "FROM NestedItemTable");
    }

    @Test
    public void testProjectFieldAccessWithITEM() {
        util().verifyRelPlan(
                        "SELECT "
                                + "`Result`.data_arr[ID].`value`, "
                                + "`Result`.data_map['item'].`value`, "
                                + "`outer_array`[1], "
                                + "`outer_array`[ID], "
                                + "`outer_map`['item'] "
                                + "FROM ItemTable");
    }

    @Test
    public void testMetadataProjectionWithoutProjectionPushDownWhenSupported() {
        final SharedReference<List<String>> appliedKeys = sharedObjects.add(new ArrayList<>());
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(NoPushDownSource.SCHEMA)
                        .source(new NoPushDownSource(true, appliedKeys))
                        .build();
        util().tableEnv().createTable("T1", sourceDescriptor);

        util().verifyRelPlan("SELECT m1, metadata FROM T1");
        assertThat(appliedKeys.get()).contains("m1", "m2");
    }

    @Test
    public void testMetadataProjectionWithoutProjectionPushDownWhenNotSupported() {
        final SharedReference<List<String>> appliedKeys = sharedObjects.add(new ArrayList<>());
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(NoPushDownSource.SCHEMA)
                        .source(new NoPushDownSource(false, appliedKeys))
                        .build();
        util().tableEnv().createTable("T2", sourceDescriptor);

        util().verifyRelPlan("SELECT m1, metadata FROM T2");
        assertThat(appliedKeys.get()).contains("m1", "m2", "m3");
    }

    @Test
    public void testMetadataProjectionWithoutProjectionPushDownWhenSupportedAndNoneSelected() {
        final SharedReference<List<String>> appliedKeys = sharedObjects.add(new ArrayList<>());
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(NoPushDownSource.SCHEMA)
                        .source(new NoPushDownSource(true, appliedKeys))
                        .build();
        util().tableEnv().createTable("T3", sourceDescriptor);

        util().verifyRelPlan("SELECT 1 FROM T3");
        // Because we turned off the project merge in the sql2rel phase, the source node will see
        // the original unmerged project with all columns selected in this rule test
        assertThat(appliedKeys.get()).hasSize(3);
    }

    @Test
    public void testMetadataProjectionWithoutProjectionPushDownWhenNotSupportedAndNoneSelected() {
        final SharedReference<List<String>> appliedKeys = sharedObjects.add(new ArrayList<>());
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(NoPushDownSource.SCHEMA)
                        .source(new NoPushDownSource(false, appliedKeys))
                        .build();
        util().tableEnv().createTable("T4", sourceDescriptor);

        util().verifyRelPlan("SELECT 1 FROM T4");
        assertThat(appliedKeys.get()).contains("m1", "m2", "m3");
    }

    @Test
    public void testProjectionIncludingOnlyMetadata() {
        replaceProgramWithProjectMergeRule();

        final AtomicReference<DataType> appliedProjectionDataType = new AtomicReference<>(null);
        final AtomicReference<DataType> appliedMetadataDataType = new AtomicReference<>(null);
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(PushDownSource.SCHEMA)
                        .source(
                                new PushDownSource(
                                        appliedProjectionDataType, appliedMetadataDataType))
                        .build();
        util().tableEnv().createTable("T5", sourceDescriptor);

        util().verifyRelPlan("SELECT metadata FROM T5");

        assertThat(appliedProjectionDataType.get()).isNotNull();
        assertThat(appliedMetadataDataType.get()).isNotNull();

        assertThat(DataType.getFieldNames(appliedProjectionDataType.get())).isEmpty();
        assertThat(DataType.getFieldNames(appliedMetadataDataType.get()))
                .containsExactly("metadata");
    }

    private void replaceProgramWithProjectMergeRule() {
        FlinkChainedProgram programs = new FlinkChainedProgram<BatchOptimizeContext>();
        programs.addLast(
                "rules",
                FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(
                                RuleSets.ofList(
                                        CoreRules.PROJECT_MERGE,
                                        PushProjectIntoTableSourceScanRule.INSTANCE))
                        .build());
        util().replaceBatchProgram(programs);
    }

    @Test
    public void testProjectionWithMetadataAndPhysicalFields() {
        replaceProgramWithProjectMergeRule();

        final AtomicReference<DataType> appliedProjectionDataType = new AtomicReference<>(null);
        final AtomicReference<DataType> appliedMetadataDataType = new AtomicReference<>(null);
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(PushDownSource.SCHEMA)
                        .source(
                                new PushDownSource(
                                        appliedProjectionDataType, appliedMetadataDataType))
                        .build();
        util().tableEnv().createTable("T5", sourceDescriptor);

        util().verifyRelPlan("SELECT metadata, f1 FROM T5");

        assertThat(appliedProjectionDataType.get()).isNotNull();
        assertThat(appliedMetadataDataType.get()).isNotNull();

        assertThat(DataType.getFieldNames(appliedProjectionDataType.get())).containsExactly("f1");
        assertThat(DataType.getFieldNames(appliedMetadataDataType.get()))
                .isEqualTo(Arrays.asList("f1", "metadata"));
    }

    // ---------------------------------------------------------------------------------------------

    /** Source which supports metadata but not {@link SupportsProjectionPushDown}. */
    private static class NoPushDownSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .columnByMetadata("m1", STRING())
                        .columnByMetadata("metadata", STRING(), "m2")
                        .columnByMetadata("m3", STRING())
                        .build();

        private final boolean supportsMetadataProjection;
        private final SharedReference<List<String>> appliedMetadataKeys;

        public NoPushDownSource(
                boolean supportsMetadataProjection,
                SharedReference<List<String>> appliedMetadataKeys) {
            this.supportsMetadataProjection = supportsMetadataProjection;
            this.appliedMetadataKeys = appliedMetadataKeys;
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            final Map<String, DataType> metadata = new HashMap<>();
            metadata.put("m1", STRING());
            metadata.put("m2", STRING());
            metadata.put("m3", STRING());
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
            appliedMetadataKeys.get().clear();
            appliedMetadataKeys.get().addAll(metadataKeys);
        }

        @Override
        public boolean supportsMetadataProjection() {
            return supportsMetadataProjection;
        }
    }

    /**
     * Source which supports both {@link SupportsProjectionPushDown} and {@link
     * SupportsReadingMetadata}.
     */
    private static class PushDownSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata, SupportsProjectionPushDown {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("f1", STRING())
                        .columnByMetadata("metadata", STRING(), "m2")
                        .columnByMetadata("m3", STRING())
                        .build();

        private final AtomicReference<DataType> appliedProjectionType;
        private final AtomicReference<DataType> appliedMetadataType;

        private PushDownSource(
                AtomicReference<DataType> appliedProjectionType,
                AtomicReference<DataType> appliedMetadataType) {
            this.appliedProjectionType = appliedProjectionType;
            this.appliedMetadataType = appliedMetadataType;
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            final Map<String, DataType> metadata = new HashMap<>();
            metadata.put("m1", STRING());
            metadata.put("m2", STRING());
            metadata.put("m3", STRING());
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
            appliedMetadataType.set(producedDataType);
        }

        @Override
        public boolean supportsNestedProjection() {
            return false;
        }

        @Override
        public void applyProjection(int[][] projectedFields, DataType producedDataType) {
            appliedProjectionType.set(producedDataType);
        }
    }
}
