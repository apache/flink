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
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for CatalogPropertiesUtil. */
public class CatalogPropertiesUtilTest {
    @Test
    public void testCatalogModelSerde() {
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
                CatalogPropertiesUtil.serializeResolvedCatalogModel(testModel);
        final CatalogModel deserializedModel =
                CatalogPropertiesUtil.deserializeCatalogModel(serializedMap);

        // We need to compare the fields one by one since equalsTo in UnresolvedDataType in
        // UnresolvedColumn doesn't work
        BiConsumer<Schema, Schema> schemaComparision =
                (actual, expected) -> {
                    assertThat(actual.getColumns().size()).isEqualTo(expected.getColumns().size());
                    for (int i = 0; i < actual.getColumns().size(); i++) {
                        final UnresolvedPhysicalColumn actualColumn =
                                (UnresolvedPhysicalColumn) actual.getColumns().get(i);
                        final UnresolvedPhysicalColumn expectedColumn =
                                (UnresolvedPhysicalColumn) expected.getColumns().get(i);
                        assertThat(actualColumn.getName()).isEqualTo(expectedColumn.getName());
                        assertThat(actualColumn.getDataType().toString())
                                .isEqualTo(expectedColumn.getDataType().toString());
                    }
                };
        schemaComparision.accept(deserializedModel.getInputSchema(), catalogModel.getInputSchema());
        schemaComparision.accept(
                deserializedModel.getOutputSchema(), catalogModel.getOutputSchema());

        assertThat(deserializedModel.getOptions()).isEqualTo(catalogModel.getOptions());
        assertThat(deserializedModel.getComment()).isEqualTo(catalogModel.getComment());
    }

    @Test
    public void testCatalogTableSerde() {
        final Map<String, String> options = new HashMap<>();

        final CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column(
                                                "f1",
                                                DataTypes.INT()
                                                        .getLogicalType()
                                                        .asSerializableString())
                                        .column(
                                                "f2",
                                                DataTypes.STRING()
                                                        .getLogicalType()
                                                        .asSerializableString())
                                        .primaryKey("f1")
                                        .build())
                        .comment("some comment")
                        .options(options)
                        .build();

        final Column f1 = Column.physical("f1", DataTypes.INT());
        final Column f2 = Column.physical("f2", DataTypes.STRING());
        List<Column> columns = Arrays.asList(f1, f2);
        final UniqueConstraint primaryKey =
                UniqueConstraint.primaryKey("PK_f1", Collections.singletonList("f1"));
        final ResolvedSchema schema =
                new ResolvedSchema(columns, Collections.emptyList(), primaryKey);

        final ResolvedCatalogTable testTable = new ResolvedCatalogTable(catalogTable, schema);

        final Map<String, String> serializedMap =
                CatalogPropertiesUtil.serializeCatalogTable(testTable);
        final CatalogTable deserializedTable =
                CatalogPropertiesUtil.deserializeCatalogTable(serializedMap);

        // We need to compare the fields one by one since equalsTo in UnresolvedDataType in
        // UnresolvedColumn doesn't work
        BiConsumer<Schema, Schema> schemaComparision =
                (actual, expected) -> {
                    assertThat(actual.getColumns().size()).isEqualTo(expected.getColumns().size());
                    for (int i = 0; i < actual.getColumns().size(); i++) {
                        final UnresolvedPhysicalColumn actualColumn =
                                (UnresolvedPhysicalColumn) actual.getColumns().get(i);
                        final UnresolvedPhysicalColumn expectedColumn =
                                (UnresolvedPhysicalColumn) expected.getColumns().get(i);
                        assertThat(actualColumn.getName()).isEqualTo(expectedColumn.getName());
                        assertThat(actualColumn.getDataType().toString())
                                .isEqualTo(expectedColumn.getDataType().toString());
                    }
                };
        schemaComparision.accept(
                deserializedTable.getUnresolvedSchema(), catalogTable.getUnresolvedSchema());
        // Verify that the primary key and vector index are correctly deserialized
        assertThat(deserializedTable.getUnresolvedSchema().getPrimaryKey())
                .isEqualTo(catalogTable.getUnresolvedSchema().getPrimaryKey());
    }
}
