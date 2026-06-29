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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata.MetadataFilterResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests covering the four shapes a connector can return from {@link
 * SupportsReadingMetadata#applyMetadataFilters} (accept-all, accept-none, partial accept,
 * best-effort overlap).
 *
 * <p>For each shape this test asserts both the runtime result (the Calc above the scan must
 * evaluate any {@code remaining} predicates) and the optimized plan (the scan carries the {@code
 * accepted} set, the {@code LogicalFilter} above carries the {@code remaining} set).
 */
class MetadataFilterResultShapesITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    /** Schema: {@code id INT, m0 INT METADATA, m1 INT METADATA}. */
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", INT())
                    .columnByMetadata("m0", INT())
                    .columnByMetadata("m1", INT())
                    .build();

    /**
     * Six rows that exercise both predicates {@code m0 > 0} and {@code m1 > 0}. The data covers all
     * four cells of the Cartesian product (both pass / only m0 passes / only m1 passes / neither).
     */
    private static final List<Row> ROWS =
            Arrays.asList(
                    Row.of(1, 5, 5), // both pass
                    Row.of(2, 5, -1), // only m0 passes
                    Row.of(3, -1, 5), // only m1 passes
                    Row.of(4, -1, -1), // neither passes
                    Row.of(5, 7, 9), // both pass
                    Row.of(6, 0, 0)); // neither passes (predicates are strict >)

    private static final String SQL = "SELECT id FROM %s WHERE m0 > 0 AND m1 > 0 ORDER BY id";

    /** Rows that satisfy {@code m0 > 0 AND m1 > 0}: only id=1 and id=5. */
    private static final List<Integer> EXPECTED_FILTERED_IDS = Arrays.asList(1, 5);

    private TableEnvironment tableEnv;

    @BeforeEach
    void setup() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tableEnv = TableEnvironment.create(settings);
    }

    /** Row passes both predicates {@code m0 > 0 AND m1 > 0}. */
    private static final Predicate<Row> PASS_BOTH =
            row -> ((Integer) row.getField(1)) > 0 && ((Integer) row.getField(2)) > 0;

    /** Row passes only the first predicate {@code m0 > 0}. */
    private static final Predicate<Row> PASS_M0 = row -> ((Integer) row.getField(1)) > 0;

    /** Row passes no predicate (source emits everything). */
    private static final Predicate<Row> PASS_NONE = row -> true;

    // Plan-string assertions below use two renderers: `metadataFilter=[...]` (lowercase `and`,
    // from RexNode toString) and `where=[...]` (uppercase `AND`, from RelExplainable).

    @Test
    void testAcceptAll() throws Exception {
        // Source claims it can apply both predicates and does so by emitting only matching rows.
        registerTable("T_ACCEPT_ALL", ConfigurableMetadataSource.ACCEPT_ALL, PASS_BOTH);

        assertThat(collectIds(String.format(SQL, "T_ACCEPT_ALL")))
                .containsExactlyElementsOf(EXPECTED_FILTERED_IDS);

        String explain = tableEnv.explainSql(String.format(SQL, "T_ACCEPT_ALL"));
        // Both predicates pushed onto the scan as the conjunction.
        assertThat(explain).contains("metadataFilter=[and(>(m0, 0), >(m1, 0))]");
        // No runtime Calc with a where on metadata: accepted = inputs, remaining = empty.
        assertThat(explain).doesNotContain("where=[((m0 > 0) AND (m1 > 0))]");
        assertThat(explain).doesNotContain("where=[(m0 > 0)]");
        assertThat(explain).doesNotContain("where=[(m1 > 0)]");
    }

    @Test
    void testAcceptNone() throws Exception {
        // Source rejects everything: emit all rows; runtime Calc must do all the filtering.
        registerTable("T_ACCEPT_NONE", ConfigurableMetadataSource.ACCEPT_NONE, PASS_NONE);

        assertThat(collectIds(String.format(SQL, "T_ACCEPT_NONE")))
                .containsExactlyElementsOf(EXPECTED_FILTERED_IDS);

        String explain = tableEnv.explainSql(String.format(SQL, "T_ACCEPT_NONE"));
        // Empty accepted set; full conjunction retained on the runtime Calc.
        assertThat(explain).contains("metadataFilter=[]");
        assertThat(explain).contains("where=[AND(>(m0, 0), >(m1, 0))]");
        // Nothing pushed besides the empty marker.
        assertThat(explain).doesNotContain("metadataFilter=[>(");
        assertThat(explain).doesNotContain("metadataFilter=[and(");
    }

    @Test
    void testAcceptFirstOnly() throws Exception {
        // Source applies only m0 > 0 itself; runtime Calc must apply m1 > 0.
        registerTable("T_ACCEPT_FIRST", ConfigurableMetadataSource.ACCEPT_FIRST_ONLY, PASS_M0);

        assertThat(collectIds(String.format(SQL, "T_ACCEPT_FIRST")))
                .containsExactlyElementsOf(EXPECTED_FILTERED_IDS);

        String explain = tableEnv.explainSql(String.format(SQL, "T_ACCEPT_FIRST"));
        // First predicate (m0 > 0) is pushed; second (m1 > 0) stays on the runtime Calc.
        assertThat(explain).contains("metadataFilter=[>(m0, 0)]");
        assertThat(explain).contains("where=[>(m1, 0)]");
        assertThat(explain).doesNotContain("metadataFilter=[>(m1, 0)]");
        assertThat(explain).doesNotContain("metadataFilter=[and(");
    }

    @Test
    void testBestEffortOverlap() throws Exception {
        // Source claims both for storage-side pruning AND remains them: runtime must re-apply.
        // Source emits all rows so the runtime Calc is the load-bearing filter; correctness must
        // not depend on the source's claim.
        registerTable(
                "T_BEST_EFFORT", ConfigurableMetadataSource.BEST_EFFORT_BOTH_AND_REMAIN, PASS_NONE);

        assertThat(collectIds(String.format(SQL, "T_BEST_EFFORT")))
                .containsExactlyElementsOf(EXPECTED_FILTERED_IDS);

        String explain = tableEnv.explainSql(String.format(SQL, "T_BEST_EFFORT"));
        // Both predicates accepted (for storage-side pruning) AND both also retained as a runtime
        // Calc so the source's pruning need only be best-effort.
        assertThat(explain).contains("metadataFilter=[and(>(m0, 0), >(m1, 0))]");
        assertThat(explain).contains("where=[AND(>(m0, 0), >(m1, 0))]");
    }

    /**
     * A mixed OR predicate like {@code id > 0 OR m0 > 0} references both physical and metadata
     * columns in a single predicate that AND-decomposition cannot split. Such predicates must NOT
     * leak into the physical {@code filter=[...]} path because {@code FilterPushDownSpec} stores
     * RexInputRef indices without a row type — metadata column indices break during compiled-plan
     * restore when {@code ProjectPushDownSpec} narrows the scan type.
     *
     * <p>The mixed predicate must stay as a runtime Calc ({@code where=[...]}).
     */
    @Test
    void testMixedOrPredicateStaysAsRuntimeFilter() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", INT())
                        .columnByMetadata("m0", INT())
                        .columnByMetadata("m1", INT())
                        .build();

        MixedFilterTrackingSource source = new MixedFilterTrackingSource();
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder().schema(schema).source(source).build();
        tableEnv.createTable("T_MIXED_OR", descriptor);

        // id < 3: rows 1,2.  m0 > 0: rows 1,2,5.  Union: rows 1,2,5.
        String sql = "SELECT id FROM T_MIXED_OR WHERE id < 3 OR m0 > 0 ORDER BY id";

        String explain = tableEnv.explainSql(sql);
        assertThat(explain)
                .as("Mixed OR must not leak into physical filter path")
                .doesNotContain("filter=[OR(");
        assertThat(explain)
                .as("Mixed OR must not leak into metadata filter path")
                .doesNotContain("metadataFilter=[OR(");

        // Row data: (1,5,5), (2,5,-1), (3,-1,5), (4,-1,-1), (5,7,9), (6,0,0)
        assertThat(collectIds(sql)).containsExactly(1, 2, 5);
    }

    /**
     * When AND connects a pure physical predicate and a mixed OR, the physical predicate should be
     * pushed while the mixed OR stays as a runtime Calc.
     */
    @Test
    void testMixedOrWithSeparatePhysicalPredicate() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", INT())
                        .columnByMetadata("m0", INT())
                        .columnByMetadata("m1", INT())
                        .build();

        MixedFilterTrackingSource source = new MixedFilterTrackingSource();
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder().schema(schema).source(source).build();
        tableEnv.createTable("T_MIXED_OR2", descriptor);

        // id > 2 (physical, pushable) AND (id < 5 OR m0 > 0) (mixed, must stay as runtime)
        String sql = "SELECT id FROM T_MIXED_OR2 WHERE id > 2 AND (id < 5 OR m0 > 0) ORDER BY id";

        String explain = tableEnv.explainSql(sql);
        // The mixed OR must NOT appear in filter= or metadataFilter=
        assertThat(explain)
                .as("Mixed OR must not leak into physical filter path")
                .doesNotContain("filter=[OR(");

        // Row data: (1,5,5), (2,5,-1), (3,-1,5), (4,-1,-1), (5,7,9), (6,0,0)
        // id>2: rows 3,4,5,6.  AND (id<5 OR m0>0): row3(yes,no→yes), row4(yes,no→yes),
        // row5(no,yes→yes), row6(no,no→no).  Result: 3,4,5
        assertThat(collectIds(sql)).containsExactly(3, 4, 5);
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private void registerTable(
            String name,
            Function<List<ResolvedExpression>, MetadataFilterResult> splitter,
            Predicate<Row> sourceSideFilter) {
        TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(SCHEMA)
                        .source(new ConfigurableMetadataSource(splitter, sourceSideFilter))
                        .build();
        tableEnv.createTable(name, descriptor);
    }

    private List<Integer> collectIds(String sql) {
        TableResult result = tableEnv.executeSql(sql);
        try (CloseableIterator<Row> iterator = result.collect()) {
            List<Row> rows = CollectionUtil.iteratorToList(iterator);
            return rows.stream().map(r -> (Integer) r.getField(0)).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Test source
    // -----------------------------------------------------------------------------------------

    /**
     * Bounded harness source. Supports {@link SupportsReadingMetadata} with two metadata columns
     * ({@code m0}, {@code m1}). The {@code MetadataFilterResult} returned from {@link
     * #applyMetadataFilters(List)} is controlled by the supplied splitter strategy so each test can
     * exercise a different shape.
     *
     * <p>The runtime emits all rows; correctness for {@code remaining} predicates depends on the
     * Calc above the scan to drop non-matching rows.
     */
    static class ConfigurableMetadataSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata {

        static final Function<List<ResolvedExpression>, MetadataFilterResult> ACCEPT_ALL =
                in -> MetadataFilterResult.of(in, Collections.emptyList());

        static final Function<List<ResolvedExpression>, MetadataFilterResult> ACCEPT_NONE =
                in -> MetadataFilterResult.of(Collections.emptyList(), in);

        static final Function<List<ResolvedExpression>, MetadataFilterResult> ACCEPT_FIRST_ONLY =
                in ->
                        MetadataFilterResult.of(
                                Collections.singletonList(in.get(0)),
                                Collections.singletonList(in.get(1)));

        static final Function<List<ResolvedExpression>, MetadataFilterResult>
                BEST_EFFORT_BOTH_AND_REMAIN = in -> MetadataFilterResult.of(in, in);

        private final Function<List<ResolvedExpression>, MetadataFilterResult> splitter;
        private final Predicate<Row> sourceSideFilter;

        private DataType producedDataType;

        ConfigurableMetadataSource(
                Function<List<ResolvedExpression>, MetadataFilterResult> splitter,
                Predicate<Row> sourceSideFilter) {
            super(true);
            this.splitter = splitter;
            this.sourceSideFilter = sourceSideFilter;
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new LinkedHashMap<>();
            metadata.put("m0", INT());
            metadata.put("m1", INT());
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
            this.producedDataType = producedDataType;
        }

        @Override
        public boolean supportsMetadataFilterPushDown() {
            return true;
        }

        @Override
        public MetadataFilterResult applyMetadataFilters(List<ResolvedExpression> metadataFilters) {
            return splitter.apply(metadataFilters);
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            DataType emitted =
                    producedDataType != null
                            ? producedDataType
                            : getFactoryContext().getPhysicalRowDataType();
            DynamicTableSource.DataStructureConverter converter =
                    runtimeProviderContext.createDataStructureConverter(emitted);
            // Apply the source-side filter once; the generator just hands out rows by index.
            List<Row> emittedRows =
                    ROWS.stream().filter(sourceSideFilter).collect(Collectors.toList());
            GeneratorFunction<Long, RowData> generator =
                    index -> (RowData) converter.toInternal(emittedRows.get(index.intValue()));
            DataGeneratorSource<RowData> source =
                    new DataGeneratorSource<>(
                            generator, emittedRows.size(), TypeInformation.of(RowData.class));
            return SourceProvider.of(source);
        }
    }

    /**
     * Source that supports both physical and metadata filter push-down. Accepts all filters it
     * receives, emits all rows (runtime Calc is the load-bearing filter for remaining predicates).
     */
    static class MixedFilterTrackingSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsReadingMetadata, SupportsFilterPushDown {

        private DataType producedDataType;

        MixedFilterTrackingSource() {
            super(true);
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metadata = new LinkedHashMap<>();
            metadata.put("m0", INT());
            metadata.put("m1", INT());
            return metadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
            this.producedDataType = producedDataType;
        }

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
            return Result.of(Collections.emptyList(), filters);
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            DataType emitted =
                    producedDataType != null
                            ? producedDataType
                            : getFactoryContext().getPhysicalRowDataType();
            DynamicTableSource.DataStructureConverter converter =
                    runtimeProviderContext.createDataStructureConverter(emitted);
            GeneratorFunction<Long, RowData> generator =
                    index -> (RowData) converter.toInternal(ROWS.get(index.intValue()));
            DataGeneratorSource<RowData> source =
                    new DataGeneratorSource<>(
                            generator, ROWS.size(), TypeInformation.of(RowData.class));
            return SourceProvider.of(source);
        }
    }
}
