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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.abilities.source.LimitPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link TemporalTableSourceSpec} serialization and deserialization. */
@Execution(CONCURRENT)
public class TemporalTableSourceSpecSerdeTest {
    private static final FlinkTypeFactory FACTORY =
            new FlinkTypeFactory(
                    TemporalTableSourceSpecSerdeTest.class.getClassLoader(),
                    FlinkTypeSystem.INSTANCE);

    private static final FlinkContext FLINK_CONTEXT =
            JsonSerdeTestUtil.configuredSerdeContext().getFlinkContext();

    public static Stream<TemporalTableSourceSpec> testTemporalTableSourceSpecSerde() {
        Map<String, String> options1 = new HashMap<>();
        options1.put("connector", "filesystem");
        options1.put("format", "testcsv");
        options1.put("path", "/tmp");

        final ResolvedSchema resolvedSchema1 =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("a", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        null);

        final CatalogTable catalogTable1 =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema1).build(),
                        null,
                        Collections.emptyList(),
                        options1);

        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable1, resolvedSchema1);

        RelDataType relDataType1 = FACTORY.createSqlType(SqlTypeName.BIGINT);
        LookupTableSource lookupTableSource = new TestValuesTableFactory.MockedLookupTableSource();
        TableSourceTable tableSourceTable1 =
                new TableSourceTable(
                        null,
                        relDataType1,
                        FlinkStatistic.UNKNOWN(),
                        lookupTableSource,
                        true,
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                                resolvedCatalogTable),
                        FLINK_CONTEXT,
                        FACTORY,
                        new SourceAbilitySpec[] {new LimitPushDownSpec(100)});
        TemporalTableSourceSpec temporalTableSourceSpec1 =
                new TemporalTableSourceSpec(tableSourceTable1);
        return Stream.of(temporalTableSourceSpec1);
    }

    @ParameterizedTest
    @MethodSource("testTemporalTableSourceSpecSerde")
    public void testTemporalTableSourceSpecSerde(TemporalTableSourceSpec spec) throws IOException {
        CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();
        catalogManager.createTemporaryTable(
                spec.getTableSourceSpec().getContextResolvedTable().getResolvedTable(),
                spec.getTableSourceSpec().getContextResolvedTable().getIdentifier(),
                false);

        SerdeContext serdeCtx =
                JsonSerdeTestUtil.configuredSerdeContext(catalogManager, TableConfig.getDefault());

        String json = JsonSerdeTestUtil.toJson(serdeCtx, spec);
        TemporalTableSourceSpec actual =
                JsonSerdeTestUtil.toObject(serdeCtx, json, TemporalTableSourceSpec.class);
        assertThat(actual.getTableSourceSpec().getContextResolvedTable())
                .isEqualTo(spec.getTableSourceSpec().getContextResolvedTable());
        assertThat(actual.getTableSourceSpec().getSourceAbilities())
                .isEqualTo(spec.getTableSourceSpec().getSourceAbilities());
        assertThat(actual.getOutputType()).isEqualTo(spec.getOutputType());
    }
}
