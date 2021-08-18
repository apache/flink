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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TemporalTableSourceSpec} serialization and deserialization. */
public class TemporalTableSourceSpecSerdeTest {
    private static final FlinkTypeFactory FACTORY = FlinkTypeFactory.INSTANCE();

    @Test
    public void testTemporalTableSourceSpecSerde() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        SerdeContext serdeCtx =
                new SerdeContext(
                        new FlinkContextImpl(
                                false,
                                TableConfig.getDefault(),
                                null,
                                CatalogManagerMocks.createEmptyCatalogManager(),
                                null),
                        classLoader,
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);

        SimpleModule module = new SimpleModule();
        module.addSerializer(new RexNodeJsonSerializer());
        module.addSerializer(new RelDataTypeJsonSerializer());
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        mapper.registerModule(module);
        StringWriter writer = new StringWriter(100);
        List<TemporalTableSourceSpec> specs = testData();
        for (TemporalTableSourceSpec spec : specs) {
            try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
                gen.writeObject(spec);
            }
            String json = writer.toString();
            TemporalTableSourceSpec actual = mapper.readValue(json, TemporalTableSourceSpec.class);
            assertEquals(spec.getTableSourceSpec(), actual.getTableSourceSpec());
            assertEquals(spec.getOutputType(), actual.getOutputType());
        }
    }

    public static List<TemporalTableSourceSpec> testData() {
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
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable1, resolvedSchema1);

        RelDataType relDataType1 = FACTORY.createSqlType(SqlTypeName.BIGINT);
        LookupTableSource lookupTableSource = new TestValuesTableFactory.MockedLookupTableSource();
        TableSourceTable tableSourceTable1 =
                new TableSourceTable(
                        null,
                        ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                        relDataType1,
                        FlinkStatistic.UNKNOWN(),
                        lookupTableSource,
                        true,
                        resolvedCatalogTable,
                        new String[] {},
                        new SourceAbilitySpec[] {});
        TemporalTableSourceSpec temporalTableSourceSpec1 =
                new TemporalTableSourceSpec(tableSourceTable1, new TableConfig());
        return Arrays.asList(temporalTableSourceSpec1);
    }
}
