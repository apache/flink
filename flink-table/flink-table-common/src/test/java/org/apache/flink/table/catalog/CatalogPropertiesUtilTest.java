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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.expressions.DefaultSqlFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for CatalogPropertiesUtil. */
class CatalogPropertiesUtilTest {
    @Test
    void testCatalogModelSerde() {
        final Map<String, String> options = new HashMap<>();
        options.put("endpoint", "someendpoint");
        options.put("api_key", "fake_key");

        final CatalogModel catalogModel =
                CatalogModel.of(
                        Schema.newBuilder()
                                .column(
                                        "f1",
                                        DataTypes.INT().getLogicalType().asSerializableString())
                                .column(
                                        "f2",
                                        DataTypes.STRING().getLogicalType().asSerializableString())
                                .build(),
                        Schema.newBuilder()
                                .column(
                                        "label",
                                        DataTypes.STRING().getLogicalType().asSerializableString())
                                .build(),
                        options,
                        "some comment");

        final Column f1 = Column.physical("f1", DataTypes.INT());
        final Column f2 = Column.physical("f2", DataTypes.STRING());
        final Column label = Column.physical("label", DataTypes.STRING());
        final ResolvedSchema inputSchema = ResolvedSchema.of(f1, f2);
        final ResolvedSchema outputSchema = ResolvedSchema.of(label);

        final ResolvedCatalogModel testModel =
                ResolvedCatalogModel.of(catalogModel, inputSchema, outputSchema);

        final Map<String, String> serializedMap =
                CatalogPropertiesUtil.serializeResolvedCatalogModel(
                        testModel, DefaultSqlFactory.INSTANCE);
        final CatalogModel deserializedModel =
                CatalogPropertiesUtil.deserializeCatalogModel(serializedMap);

        assertThat(deserializedModel.getInputSchema().toString())
                .isEqualTo(catalogModel.getInputSchema().toString());
        assertThat(deserializedModel.getOutputSchema().toString())
                .isEqualTo(catalogModel.getOutputSchema().toString());
        assertThat(deserializedModel.getOptions()).isEqualTo(catalogModel.getOptions());
        assertThat(deserializedModel.getComment()).isEqualTo(catalogModel.getComment());
    }

    @ParameterizedTest
    @MethodSource("resolvedCatalogBaseTables")
    void testCatalogTableSerde(ResolvedCatalogBaseTable testTable) {
        final Map<String, String> serializedMap;
        final CatalogBaseTable deserializedTable;
        if (testTable instanceof ResolvedCatalogTable) {
            serializedMap =
                    CatalogPropertiesUtil.serializeCatalogTable(
                            (ResolvedCatalogTable) testTable, DefaultSqlFactory.INSTANCE);
            deserializedTable = CatalogPropertiesUtil.deserializeCatalogTable(serializedMap);
        } else {
            serializedMap =
                    CatalogPropertiesUtil.serializeCatalogMaterializedTable(
                            (ResolvedCatalogMaterializedTable) testTable,
                            DefaultSqlFactory.INSTANCE);
            deserializedTable =
                    CatalogPropertiesUtil.deserializeCatalogMaterializedTable(serializedMap);
        }

        assertThat(deserializedTable.getUnresolvedSchema().toString())
                .isEqualTo(testTable.getOrigin().getUnresolvedSchema().toString());

        // So far equals doesn't work as UnresolvedDataType doesn't implement equals/hashCode
        assertThat(testTable.getOrigin()).hasToString(deserializedTable.toString());
    }

    private static Stream<ResolvedCatalogBaseTable<?>> resolvedCatalogBaseTables() {
        final Schema schema =
                Schema.newBuilder()
                        .column("f1", DataTypes.INT().getLogicalType().asSerializableString())
                        .column("f2", DataTypes.STRING().getLogicalType().asSerializableString())
                        .primaryKey("f1")
                        .indexNamed("f1", Collections.singletonList("f1"))
                        .build();

        final TableDistribution hashDist =
                TableDistribution.of(TableDistribution.Kind.HASH, 2, List.of("f2"));
        final TableDistribution rangeDist =
                TableDistribution.of(TableDistribution.Kind.RANGE, 2, List.of("f1"));
        final TableDistribution unknownDist =
                TableDistribution.of(TableDistribution.Kind.UNKNOWN, 2, List.of("f1", "f2"));

        final Column f1 = Column.physical("f1", DataTypes.INT());
        final Column f2 = Column.physical("f2", DataTypes.STRING());
        List<Column> columns = Arrays.asList(f1, f2);
        final UniqueConstraint primaryKey =
                UniqueConstraint.primaryKey("PK_f1", Collections.singletonList("f1"));
        List<Index> indexes =
                Collections.singletonList(
                        DefaultIndex.newIndex("f1", Collections.singletonList("f1")));
        final ResolvedSchema resolvedSchema =
                new ResolvedSchema(columns, Collections.emptyList(), primaryKey, indexes);

        return Stream.of(
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(schema)
                                .comment("some comment")
                                .distribution(hashDist)
                                .build(),
                        resolvedSchema),
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(schema)
                                .comment("some comment")
                                .distribution(rangeDist)
                                .build(),
                        resolvedSchema),
                new ResolvedCatalogMaterializedTable(
                        CatalogMaterializedTable.newBuilder()
                                .schema(schema)
                                .comment("some comment")
                                .distribution(unknownDist)
                                .originalQuery("SELECT 1, 'two'")
                                .expandedQuery("SELECT 1, 'two'")
                                .freshness(IntervalFreshness.ofHour("123"))
                                .logicalRefreshMode(LogicalRefreshMode.CONTINUOUS)
                                .refreshMode(RefreshMode.CONTINUOUS)
                                .refreshStatus(RefreshStatus.ACTIVATED)
                                .refreshHandlerDescription("description")
                                .build(),
                        resolvedSchema,
                        RefreshMode.CONTINUOUS,
                        IntervalFreshness.ofHour("123")));
    }
}
