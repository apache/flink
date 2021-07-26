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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.PartitioningSpec;
import org.apache.flink.table.planner.plan.abilities.sink.WritingMetadataSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Tests for {@link DynamicTableSinkSpec} serialization and deserialization. */
@RunWith(Parameterized.class)
public class DynamicTableSinkSpecSerdeTest {

    @Parameterized.Parameter public DynamicTableSinkSpec spec;

    @Test
    public void testDynamicTableSinkSpecSerde() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        SerdeContext serdeCtx =
                new SerdeContext(
                        new FlinkContextImpl(
                                TableConfig.getDefault(),
                                null,
                                CatalogManagerMocks.createEmptyCatalogManager(),
                                null),
                        classLoader,
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(spec);
        }
        String json = writer.toString();
        DynamicTableSinkSpec actual = mapper.readValue(json, DynamicTableSinkSpec.class);
        assertEquals(spec, actual);
        assertNull(actual.getClassLoader());
        actual.setClassLoader(classLoader);
        assertNull(actual.getReadableConfig());
        actual.setReadableConfig(serdeCtx.getConfiguration());
        assertNotNull(actual.getTableSink());
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<DynamicTableSinkSpec> testData() {
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("connector", "filesystem");
        properties1.put("format", "testcsv");
        properties1.put("path", "/tmp");
        properties1.put("schema.0.name", "a");
        properties1.put("schema.0.data-type", "BIGINT");

        final CatalogTable catalogTable1 = CatalogTable.fromProperties(properties1);

        final ResolvedSchema resolvedSchema1 =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("a", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        null);

        DynamicTableSinkSpec spec1 =
                new DynamicTableSinkSpec(
                        ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                        new ResolvedCatalogTable(catalogTable1, resolvedSchema1),
                        Collections.emptyList());
        spec1.setReadableConfig(new Configuration());

        Map<String, String> properties2 = new HashMap<>();
        properties2.put("connector", "filesystem");
        properties2.put("format", "testcsv");
        properties2.put("path", "/tmp");
        properties2.put("schema.0.name", "a");
        properties2.put("schema.0.data-type", "BIGINT");
        properties2.put("schema.1.name", "b");
        properties2.put("schema.1.data-type", "INT");
        properties2.put("schema.2.name", "p");
        properties2.put("schema.2.data-type", "STRING");

        final CatalogTable catalogTable2 = CatalogTable.fromProperties(properties2);

        final ResolvedSchema resolvedSchema2 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("p", DataTypes.STRING())),
                        Collections.emptyList(),
                        null);

        DynamicTableSinkSpec spec2 =
                new DynamicTableSinkSpec(
                        ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                        new ResolvedCatalogTable(catalogTable2, resolvedSchema2),
                        Arrays.asList(
                                new OverwriteSpec(true),
                                new PartitioningSpec(
                                        new HashMap<String, String>() {
                                            {
                                                put("p", "A");
                                            }
                                        })));
        spec2.setReadableConfig(new Configuration());

        Map<String, String> properties3 = new HashMap<>();
        properties3.put("connector", "values");
        properties3.put("schema.0.name", "a");
        properties3.put("schema.0.data-type", "BIGINT");
        properties3.put("schema.1.name", "b");
        properties3.put("schema.1.data-type", "INT");
        properties3.put("schema.2.name", "m");
        properties3.put("schema.2.data-type", "STRING");
        properties3.put("writable-metadata", "m:STRING");

        final CatalogTable catalogTable3 = CatalogTable.fromProperties(properties3);

        final ResolvedSchema resolvedSchema3 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("m", DataTypes.STRING())),
                        Collections.emptyList(),
                        null);

        DynamicTableSinkSpec spec3 =
                new DynamicTableSinkSpec(
                        ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                        new ResolvedCatalogTable(catalogTable3, resolvedSchema3),
                        Collections.singletonList(
                                new WritingMetadataSpec(
                                        Collections.singletonList("m"),
                                        RowType.of(new BigIntType(), new IntType()))));
        spec3.setReadableConfig(new Configuration());

        return Arrays.asList(spec1, spec2, spec3);
    }
}
