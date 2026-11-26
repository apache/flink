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

package org.apache.flink.table.api;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.ExpressionResolverMocks;
import org.apache.flink.table.utils.ParserMock;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MaterializedTable}. */
class MaterializedTableTest {

    private static final ObjectIdentifier IDENTIFIER =
            ObjectIdentifier.of("default_catalog", "default_database", "mt");

    private TableEnvironmentMock tEnv;
    private Catalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        tEnv = TableEnvironmentMock.getStreamingInstance();
        catalog = tEnv.getCatalog(IDENTIFIER.getCatalogName()).orElseThrow(AssertionError::new);
        catalog.createTable(
                IDENTIFIER.toObjectPath(),
                createMaterializedTable(
                        "SELECT 1 AS f0", "SELECT 1 AS `f0`", RefreshStatus.ACTIVATED, "job 1"),
                false);
    }

    @Test
    void testBasic() {
        final MaterializedTable materializedTable = tEnv.fromMaterializedTable("mt");

        assertThat(materializedTable.getIdentifier()).isEqualTo(IDENTIFIER);
        assertThat(materializedTable.getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(materializedTable.getOriginalQuery()).isEqualTo("SELECT 1 AS f0");
        assertThat(materializedTable.getExpandedQuery()).isEqualTo("SELECT 1 AS `f0`");
        assertThat(materializedTable.getFreshness()).isEqualTo(IntervalFreshness.ofSecond("30"));
        assertThat(materializedTable.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);
        assertThat(materializedTable.getRefreshHandlerDescription()).hasValue("job 1");
    }

    @Test
    void testGettersReadTheCatalogOnEveryCall() throws Exception {
        final MaterializedTable materializedTable = tEnv.fromMaterializedTable("mt");

        catalog.alterTable(
                IDENTIFIER.toObjectPath(),
                createMaterializedTable(
                        "SELECT 2 AS f0", "SELECT 2 AS `f0`", RefreshStatus.SUSPENDED, "job 2"),
                false);

        assertThat(materializedTable.getExpandedQuery()).isEqualTo("SELECT 2 AS `f0`");
        assertThat(materializedTable.getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
        assertThat(materializedTable.getRefreshHandlerDescription()).hasValue("job 2");
    }

    @Test
    void testToDescriptor() throws Exception {
        tEnv.getCatalogManager()
                .initSchemaResolver(
                        true,
                        ExpressionResolverMocks.forSqlExpression(
                                MaterializedTableTest::resolveSqlExpression),
                        new ParserMock());
        catalog.createTable(
                new ObjectPath(IDENTIFIER.getDatabaseName(), "described_mt"),
                CatalogMaterializedTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("k", DataTypes.INT().notNull())
                                        .withComment("key")
                                        .column("ds", DataTypes.STRING().notNull())
                                        .column("ts", DataTypes.TIMESTAMP(3))
                                        .columnByMetadata(
                                                "part", DataTypes.INT(), "partition", true)
                                        .columnByMetadata(
                                                "headers", DataTypes.STRING(), null, false)
                                        .columnByExpression("k_plus_one", "k + 1")
                                        .withComment("derived")
                                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                                        .primaryKeyNamed("pk", "k", "ds")
                                        .build())
                        .options(Map.of("connector", "values"))
                        .distribution(TableDistribution.ofHash(List.of("k"), 4))
                        .partitionKeys(List.of("ds"))
                        .comment("described")
                        .originalQuery("SELECT 1")
                        .expandedQuery("SELECT 1")
                        .freshness(IntervalFreshness.ofSecond("30"))
                        .logicalRefreshMode(LogicalRefreshMode.CONTINUOUS)
                        .refreshMode(RefreshMode.CONTINUOUS)
                        .refreshStatus(RefreshStatus.ACTIVATED)
                        .startMode(StartMode.fromBeginning())
                        .build(),
                false);

        assertThat(tEnv.fromMaterializedTable("described_mt").toDescriptor())
                .isEqualTo(
                        MaterializedTableDescriptor.newBuilder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("k", DataTypes.INT().notNull())
                                                .withComment("key")
                                                .column("ds", DataTypes.STRING().notNull())
                                                .column("ts", DataTypes.TIMESTAMP(3))
                                                .columnByMetadata(
                                                        "part", DataTypes.INT(), "partition", true)
                                                .columnByMetadata(
                                                        "headers", DataTypes.STRING(), null, false)
                                                .columnByExpression("k_plus_one", "`k` + 1")
                                                .withComment("derived")
                                                .watermark("ts", "`ts` - INTERVAL '5' SECOND")
                                                .primaryKeyNamed("pk", "k", "ds")
                                                .build())
                                .option("connector", "values")
                                .distributedByHash(4, "k")
                                .partitionedBy("ds")
                                .comment("described")
                                .freshness(IntervalFreshness.ofSecond("30"))
                                .continuousRefresh()
                                .startMode(StartMode.fromBeginning())
                                .build());
    }

    @ParameterizedTest
    @MethodSource("distributions")
    void testToDescriptorKeepsTheDistribution(TableDistribution distribution) throws Exception {
        catalog.createTable(
                new ObjectPath(IDENTIFIER.getDatabaseName(), "distributed_mt"),
                CatalogMaterializedTable.newBuilder()
                        .schema(Schema.newBuilder().column("k", DataTypes.INT()).build())
                        .options(Map.of())
                        .distribution(distribution)
                        .originalQuery("SELECT 1 AS k")
                        .expandedQuery("SELECT 1 AS `k`")
                        .freshness(IntervalFreshness.ofSecond("30"))
                        .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                        .refreshMode(RefreshMode.CONTINUOUS)
                        .refreshStatus(RefreshStatus.ACTIVATED)
                        .build(),
                false);

        assertThat(tEnv.fromMaterializedTable("distributed_mt").toDescriptor().getDistribution())
                .hasValue(distribution);
    }

    private static Stream<TableDistribution> distributions() {
        return Stream.of(
                TableDistribution.ofUnknown(4),
                TableDistribution.ofUnknown(List.of("k"), null),
                TableDistribution.ofUnknown(List.of("k"), 4),
                TableDistribution.ofHash(List.of("k"), null),
                TableDistribution.ofHash(List.of("k"), 4),
                TableDistribution.ofRange(List.of("k"), null),
                TableDistribution.ofRange(List.of("k"), 4));
    }

    @Test
    void testToDescriptorSerializesInlineFunctionsWithTheSessionSqlFactory() throws Exception {
        tEnv.getCatalogManager()
                .initSchemaResolver(
                        true,
                        ExpressionResolverMocks.forSqlExpression(
                                MaterializedTableTest::resolveSqlExpression),
                        new ParserMock());
        tEnv.getCatalogManager().setSqlFactory(functionDefinition -> "`inline_plus_one`");
        catalog.createTable(
                new ObjectPath(IDENTIFIER.getDatabaseName(), "inline_mt"),
                CatalogMaterializedTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("v", DataTypes.BIGINT())
                                        .columnByExpression("v_plus_one", "plus_one(v)")
                                        .build())
                        .options(Map.of())
                        .originalQuery("SELECT 1")
                        .expandedQuery("SELECT 1")
                        .freshness(IntervalFreshness.ofSecond("30"))
                        .logicalRefreshMode(LogicalRefreshMode.CONTINUOUS)
                        .refreshMode(RefreshMode.CONTINUOUS)
                        .refreshStatus(RefreshStatus.ACTIVATED)
                        .build(),
                false);

        assertThat(tEnv.fromMaterializedTable("inline_mt").toDescriptor().getSchema())
                .hasValue(
                        Schema.newBuilder()
                                .column("v", DataTypes.BIGINT())
                                .columnByExpression("v_plus_one", "`inline_plus_one`(`v`)")
                                .build());
    }

    private static ResolvedExpression resolveSqlExpression(
            String sqlExpression, RowType inputRowType, @Nullable LogicalType outputType) {
        switch (sqlExpression) {
            case "k + 1":
                return ResolvedExpressionMock.of(DataTypes.INT(), "`k` + 1");
            case "ts - INTERVAL '5' SECOND":
                return ResolvedExpressionMock.of(
                        DataTypes.TIMESTAMP(3), "`ts` - INTERVAL '5' SECOND");
            case "plus_one(v)":
                return CallExpression.anonymous(
                        new PlusOne(),
                        List.of(new FieldReferenceExpression("v", DataTypes.BIGINT(), 0, 0)),
                        DataTypes.BIGINT());
            default:
                throw new UnsupportedOperationException("Unknown SQL expression: " + sqlExpression);
        }
    }

    private static CatalogMaterializedTable createMaterializedTable(
            String originalQuery,
            String expandedQuery,
            RefreshStatus refreshStatus,
            String refreshHandlerDescription) {
        return CatalogMaterializedTable.newBuilder()
                .schema(Schema.newBuilder().column("f0", DataTypes.INT()).build())
                .options(Map.of())
                .originalQuery(originalQuery)
                .expandedQuery(expandedQuery)
                .freshness(IntervalFreshness.ofSecond("30"))
                .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                .refreshMode(RefreshMode.CONTINUOUS)
                .refreshStatus(refreshStatus)
                .refreshHandlerDescription(refreshHandlerDescription)
                .build();
    }

    /** A user-defined function for the inline function case. */
    public static class PlusOne extends ScalarFunction {
        public Long eval(Long value) {
            return value == null ? null : value + 1;
        }
    }
}
