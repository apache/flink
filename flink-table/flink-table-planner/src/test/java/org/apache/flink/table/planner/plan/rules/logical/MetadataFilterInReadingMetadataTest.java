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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata.MetadataFilterResult;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for metadata filter push-down through {@link SupportsReadingMetadata}. */
class MetadataFilterInReadingMetadataTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
        CalciteConfig calciteConfig =
                TableConfigUtils.getCalciteConfig(util.tableEnv().getConfig());
        calciteConfig
                .getBatchProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                                .setHepRulesExecutionType(
                                        HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(
                                        RuleSets.ofList(
                                                PushFilterIntoTableSourceScanRule.INSTANCE,
                                                CoreRules.FILTER_PROJECT_TRANSPOSE))
                                .build());
    }

    @Test
    void testMetadataFilterPushDown() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new MetadataFilterSource(true))
                        .build();
        util.tableEnv().createTable("T1", descriptor);

        util.verifyRelPlan("SELECT id FROM T1 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'");
    }

    @Test
    void testMetadataFilterNotPushedWhenNotSupported() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new MetadataFilterSource(false))
                        .build();
        util.tableEnv().createTable("T2", descriptor);

        util.verifyRelPlan("SELECT id FROM T2 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'");
    }

    @Test
    void testAliasedMetadataColumnFilter() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(RenamedMetadataFilterSource.SCHEMA)
                        .source(new RenamedMetadataFilterSource())
                        .build();
        util.tableEnv().createTable("T3", descriptor);

        // 'event_ts' is the SQL alias for metadata key 'timestamp'; the spec stores the metadata
        // key so the plan shows `metadataFilter=[>(timestamp, ...)]` not the alias.
        util.verifyRelPlan("SELECT id FROM T3 WHERE event_ts > TIMESTAMP '2024-01-01 00:00:00'");
    }

    @Test
    void testMixedPhysicalAndMetadataFilters() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MixedFilterSource.SCHEMA)
                        .source(new MixedFilterSource())
                        .build();
        util.tableEnv().createTable("T4", descriptor);

        // id > 10 → physical path, event_time > ... → metadata path.
        util.verifyRelPlan(
                "SELECT id FROM T4 WHERE id > 10 AND event_time > TIMESTAMP '2024-01-01 00:00:00'");
    }

    @Test
    void testPartialMetadataFilterAcceptance() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(PartialMetadataFilterSource.SCHEMA)
                        .source(new PartialMetadataFilterSource())
                        .build();
        util.tableEnv().createTable("T6", descriptor);

        // Source accepts the first filter and rejects the second.
        util.verifyRelPlan(
                "SELECT id FROM T6 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'"
                        + " AND priority > 5");
    }

    @Test
    void testPhysicalAndMetadataNameCollision() {
        // Physical column 'offset' shares a name with metadata key 'offset' (aliased to
        // 'msg_offset'). The predicate must push down using the metadata key, not the alias.
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(CollidingNameSource.SCHEMA)
                        .source(new CollidingNameSource())
                        .build();
        util.tableEnv().createTable("T7", descriptor);

        util.verifyRelPlan("SELECT id FROM T7 WHERE msg_offset > 5");
    }

    @Test
    void testBestEffortMetadataPruning() {
        // Source puts every predicate in both accepted and remaining; plan shows both paths.
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new BestEffortPruningSource())
                        .build();
        util.tableEnv().createTable("T8", descriptor);

        util.verifyRelPlan("SELECT id FROM T8 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'");
    }

    @Test
    void testNonContiguousSubsetAcceptance() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(NonContiguousAcceptingSource.SCHEMA)
                        .source(new NonContiguousAcceptingSource())
                        .build();
        util.tableEnv().createTable("T9", descriptor);

        util.verifyRelPlan("SELECT id FROM T9 WHERE m0 > 0 AND m1 > 1 AND m2 > 2");
    }

    @Test
    void testCoverageInvariantWhenSourceDropsPredicate() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new DroppingAllSource())
                        .build();
        util.tableEnv().createTable("TDrop", descriptor);

        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .explainSql(
                                                "SELECT id FROM TDrop "
                                                        + "WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Source dropped a metadata filter that was passed to "
                                + "applyMetadataFilters. Every input predicate must appear in the "
                                + "result's accepted list, remaining list, or both.");
    }

    @Test
    void testMetadataFilterWithProjection() {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new MetadataFilterSource(true))
                        .build();
        util.tableEnv().createTable("T5", descriptor);

        // Projection push-down must not perturb the metadata filter.
        util.verifyRelPlan(
                "SELECT id, name FROM T5 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'");
    }

    // -----------------------------------------------------------------------------------------
    // Test sources
    // -----------------------------------------------------------------------------------------

    /** Supports metadata filter push-down. */
    private static class MetadataFilterSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("id", INT())
                        .column("name", STRING())
                        .columnByMetadata("event_time", TIMESTAMP(3))
                        .build();

        private final boolean supportsMetadataFilter;

        MetadataFilterSource(boolean supportsMetadataFilter) {
            this.supportsMetadataFilter = supportsMetadataFilter;
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new HashMap<>();
            metadata.put("event_time", org.apache.flink.table.api.DataTypes.TIMESTAMP(3));
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return supportsMetadataFilter;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            return MetadataFilterResult.of(metadataFilters, Collections.emptyList());
        }
    }

    /** Returns each input in both accepted and remaining (best-effort pruning shape). */
    private static class BestEffortPruningSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new HashMap<>();
            metadata.put("event_time", TIMESTAMP(3));
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            // Best-effort: accepted = remaining = all.
            return MetadataFilterResult.of(metadataFilters, metadataFilters);
        }
    }

    /** Tests key translation when SQL alias differs from metadata key. */
    private static class RenamedMetadataFilterSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("id", INT())
                        .columnByMetadata("event_ts", TIMESTAMP(3), "timestamp")
                        .build();

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new HashMap<>();
            metadata.put("timestamp", org.apache.flink.table.api.DataTypes.TIMESTAMP(3));
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            return MetadataFilterResult.of(metadataFilters, Collections.emptyList());
        }
    }

    /** Accepts only the first metadata filter; rejected filters remain in plan. */
    private static class PartialMetadataFilterSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("id", INT())
                        .columnByMetadata("event_time", TIMESTAMP(3))
                        .columnByMetadata("priority", INT())
                        .build();

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new LinkedHashMap<>();
            metadata.put("event_time", TIMESTAMP(3));
            metadata.put("priority", INT());
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            List<ResolvedExpression> accepted =
                    metadataFilters.isEmpty()
                            ? Collections.emptyList()
                            : Collections.singletonList(metadataFilters.get(0));
            List<ResolvedExpression> remaining =
                    metadataFilters.size() > 1
                            ? metadataFilters.subList(1, metadataFilters.size())
                            : Collections.emptyList();
            return MetadataFilterResult.of(accepted, remaining);
        }
    }

    /** Tests mixed physical and metadata filter push-down. */
    private static class MixedFilterSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata, SupportsFilterPushDown {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("id", INT())
                        .column("name", STRING())
                        .columnByMetadata("event_time", TIMESTAMP(3))
                        .build();

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new HashMap<>();
            metadata.put("event_time", org.apache.flink.table.api.DataTypes.TIMESTAMP(3));
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            return MetadataFilterResult.of(metadataFilters, Collections.emptyList());
        }

        @Override
        public Result applyFilters(List<ResolvedExpression> filters) {
            return Result.of(filters, Collections.emptyList());
        }
    }

    /**
     * Physical column {@code offset} shares a name with the metadata key {@code offset} (SQL alias
     * {@code msg_offset}). Exercises the physical-vs-metadata name collision case.
     */
    private static class CollidingNameSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("id", INT())
                        .column("offset", INT())
                        .columnByMetadata("msg_offset", INT(), "offset")
                        .build();

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new HashMap<>();
            metadata.put("offset", INT());
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            return MetadataFilterResult.of(metadataFilters, Collections.emptyList());
        }
    }

    /** Accepts inputs at positions 0 and 2, rejects position 1. */
    private static class NonContiguousAcceptingSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("id", INT())
                        .columnByMetadata("m0", INT())
                        .columnByMetadata("m1", INT())
                        .columnByMetadata("m2", INT())
                        .build();

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new LinkedHashMap<>();
            metadata.put("m0", INT());
            metadata.put("m1", INT());
            metadata.put("m2", INT());
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            // Accept inputs at positions 0 and 2; reject position 1.
            List<ResolvedExpression> accepted = new ArrayList<>();
            List<ResolvedExpression> remaining = new ArrayList<>();
            for (int i = 0; i < metadataFilters.size(); i++) {
                if (i == 1) {
                    remaining.add(metadataFilters.get(i));
                } else {
                    accepted.add(metadataFilters.get(i));
                }
            }
            return MetadataFilterResult.of(accepted, remaining);
        }
    }

    /** Drops every input by returning empty accepted and empty remaining. */
    private static class DroppingAllSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new HashMap<>();
            metadata.put("event_time", TIMESTAMP(3));
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            return MetadataFilterResult.of(Collections.emptyList(), Collections.emptyList());
        }
    }
}
