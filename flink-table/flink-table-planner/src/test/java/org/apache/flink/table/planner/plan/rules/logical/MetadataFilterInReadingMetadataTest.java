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
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for metadata filter push-down through {@link SupportsReadingMetadata}. */
class MetadataFilterInReadingMetadataTest extends TableTestBase {

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

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
        SharedReference<List<ResolvedExpression>> receivedFilters =
                sharedObjects.add(new ArrayList<>());
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new MetadataFilterSource(true, receivedFilters))
                        .build();
        util.tableEnv().createTable("T1", descriptor);

        util.verifyRelPlan("SELECT id FROM T1 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'");

        assertThat(receivedFilters.get().toString())
                .isEqualTo("[greaterThan(event_time, 2024-01-01T00:00)]");
    }

    @Test
    void testMetadataFilterNotPushedWhenNotSupported() {
        SharedReference<List<ResolvedExpression>> receivedFilters =
                sharedObjects.add(new ArrayList<>());
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new MetadataFilterSource(false, receivedFilters))
                        .build();
        util.tableEnv().createTable("T2", descriptor);

        util.verifyRelPlan("SELECT id FROM T2 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'");

        // No metadata filters should have been pushed
        assertThat(receivedFilters.get()).isEmpty();
    }

    @Test
    void testAliasedMetadataColumnFilter() {
        SharedReference<List<ResolvedExpression>> receivedFilters =
                sharedObjects.add(new ArrayList<>());
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(RenamedMetadataFilterSource.SCHEMA)
                        .source(new RenamedMetadataFilterSource(receivedFilters))
                        .build();
        util.tableEnv().createTable("T3", descriptor);

        // 'event_ts' is the SQL alias for metadata key 'timestamp'
        util.verifyRelPlan("SELECT id FROM T3 WHERE event_ts > TIMESTAMP '2024-01-01 00:00:00'");

        // The source should receive the filter with metadata key 'timestamp', not 'event_ts'.
        assertThat(receivedFilters.get().toString())
                .isEqualTo("[greaterThan(timestamp, 2024-01-01T00:00)]");
    }

    @Test
    void testMixedPhysicalAndMetadataFilters() {
        SharedReference<List<ResolvedExpression>> metadataFilters =
                sharedObjects.add(new ArrayList<>());
        SharedReference<List<ResolvedExpression>> physicalFilters =
                sharedObjects.add(new ArrayList<>());
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MixedFilterSource.SCHEMA)
                        .source(new MixedFilterSource(metadataFilters, physicalFilters))
                        .build();
        util.tableEnv().createTable("T4", descriptor);

        util.verifyRelPlan(
                "SELECT id FROM T4 WHERE id > 10 AND event_time > TIMESTAMP '2024-01-01 00:00:00'");

        // Verify routing: id > 10 → physical path, event_time > ... → metadata path.
        assertThat(physicalFilters.get().toString()).isEqualTo("[greaterThan(id, 10)]");
        assertThat(metadataFilters.get().toString())
                .isEqualTo("[greaterThan(event_time, 2024-01-01T00:00)]");
    }

    @Test
    void testPartialMetadataFilterAcceptance() {
        SharedReference<List<ResolvedExpression>> receivedFilters =
                sharedObjects.add(new ArrayList<>());
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(PartialMetadataFilterSource.SCHEMA)
                        .source(new PartialMetadataFilterSource(receivedFilters))
                        .build();
        util.tableEnv().createTable("T6", descriptor);

        // Two metadata filters: the source accepts only the first one
        util.verifyRelPlan(
                "SELECT id FROM T6 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'"
                        + " AND priority > 5");

        // Source receives both filters; the XML reference verifies only the first is accepted
        // (the second remains as a LogicalFilter above the scan).
        assertThat(receivedFilters.get().toString())
                .isEqualTo("[greaterThan(event_time, 2024-01-01T00:00), greaterThan(priority, 5)]");
    }

    @Test
    void testPhysicalAndMetadataNameCollision() {
        // Physical column 'offset' shares a name with the metadata key 'offset'
        // (aliased in SQL as 'msg_offset'). The predicate on the metadata column
        // must be pushed down using the metadata key, not confused with the
        // physical column of the same name.
        SharedReference<List<ResolvedExpression>> receivedFilters =
                sharedObjects.add(new ArrayList<>());
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(CollidingNameSource.SCHEMA)
                        .source(new CollidingNameSource(receivedFilters))
                        .build();
        util.tableEnv().createTable("T7", descriptor);

        util.verifyRelPlan("SELECT id FROM T7 WHERE msg_offset > 5");

        // Must reference the metadata key 'offset', NOT the SQL alias 'msg_offset'.
        assertThat(receivedFilters.get().toString()).isEqualTo("[greaterThan(offset, 5)]");
    }

    @Test
    void testMetadataFilterWithProjection() {
        SharedReference<List<ResolvedExpression>> receivedFilters =
                sharedObjects.add(new ArrayList<>());
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(MetadataFilterSource.SCHEMA)
                        .source(new MetadataFilterSource(true, receivedFilters))
                        .build();
        util.tableEnv().createTable("T5", descriptor);

        util.verifyRelPlan(
                "SELECT id, name FROM T5 WHERE event_time > TIMESTAMP '2024-01-01 00:00:00'");

        // Projection push-down must not perturb the metadata filter.
        assertThat(receivedFilters.get().toString())
                .isEqualTo("[greaterThan(event_time, 2024-01-01T00:00)]");
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
        private final SharedReference<List<ResolvedExpression>> receivedMetadataFilters;

        MetadataFilterSource(
                boolean supportsMetadataFilter,
                SharedReference<List<ResolvedExpression>> receivedMetadataFilters) {
            this.supportsMetadataFilter = supportsMetadataFilter;
            this.receivedMetadataFilters = receivedMetadataFilters;
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
            receivedMetadataFilters.get().addAll(metadataFilters);
            return MetadataFilterResult.of(metadataFilters, Collections.emptyList());
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

        private final SharedReference<List<ResolvedExpression>> receivedMetadataFilters;

        RenamedMetadataFilterSource(
                SharedReference<List<ResolvedExpression>> receivedMetadataFilters) {
            this.receivedMetadataFilters = receivedMetadataFilters;
        }

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
            receivedMetadataFilters.get().addAll(metadataFilters);
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

        private final SharedReference<List<ResolvedExpression>> receivedMetadataFilters;

        PartialMetadataFilterSource(
                SharedReference<List<ResolvedExpression>> receivedMetadataFilters) {
            this.receivedMetadataFilters = receivedMetadataFilters;
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new HashMap<>();
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
            receivedMetadataFilters.get().addAll(metadataFilters);
            // Accept only the first filter
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

        private final SharedReference<List<ResolvedExpression>> receivedMetadataFilters;
        private final SharedReference<List<ResolvedExpression>> receivedPhysicalFilters;

        MixedFilterSource(
                SharedReference<List<ResolvedExpression>> receivedMetadataFilters,
                SharedReference<List<ResolvedExpression>> receivedPhysicalFilters) {
            this.receivedMetadataFilters = receivedMetadataFilters;
            this.receivedPhysicalFilters = receivedPhysicalFilters;
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
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            receivedMetadataFilters.get().addAll(metadataFilters);
            return MetadataFilterResult.of(metadataFilters, Collections.emptyList());
        }

        @Override
        public Result applyFilters(List<ResolvedExpression> filters) {
            receivedPhysicalFilters.get().addAll(filters);
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

        private final SharedReference<List<ResolvedExpression>> receivedMetadataFilters;

        CollidingNameSource(SharedReference<List<ResolvedExpression>> receivedMetadataFilters) {
            this.receivedMetadataFilters = receivedMetadataFilters;
        }

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
            receivedMetadataFilters.get().addAll(metadataFilters);
            return MetadataFilterResult.of(metadataFilters, Collections.emptyList());
        }
    }
}
