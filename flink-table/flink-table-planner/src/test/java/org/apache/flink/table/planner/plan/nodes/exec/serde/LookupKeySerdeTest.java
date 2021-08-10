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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test LookupKey json ser/de. */
public class LookupKeySerdeTest {

    @Test
    public void testLookupKey() throws JsonProcessingException {
        TableConfig tableConfig = TableConfig.getDefault();
        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(Thread.currentThread().getContextClassLoader())
                        .config(tableConfig.getConfiguration())
                        .defaultCatalog("default_catalog", new GenericInMemoryCatalog("default_db"))
                        .build();
        FlinkContext flinkContext =
                new FlinkContextImpl(
                        false,
                        tableConfig,
                        new FunctionCatalog(tableConfig, catalogManager, new ModuleManager()),
                        catalogManager,
                        null);
        SerdeContext serdeCtx =
                new SerdeContext(
                        flinkContext,
                        Thread.currentThread().getContextClassLoader(),
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        SimpleModule module = new SimpleModule();
        module.addSerializer(new LogicalTypeJsonSerializer());
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        module.addSerializer(new RexNodeJsonSerializer());
        module.addSerializer(new RelDataTypeJsonSerializer());
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        mapper.registerModule(module);

        LookupJoinUtil.LookupKey[] lookupKeys =
                new LookupJoinUtil.LookupKey[] {
                    new LookupJoinUtil.ConstantLookupKey(
                            new BigIntType(),
                            new RexBuilder(FlinkTypeFactory.INSTANCE()).makeLiteral("a")),
                    new LookupJoinUtil.FieldRefLookupKey(3)
                };
        for (LookupJoinUtil.LookupKey lookupKey : lookupKeys) {
            LookupJoinUtil.LookupKey result =
                    mapper.readValue(
                            mapper.writeValueAsString(lookupKey), LookupJoinUtil.LookupKey.class);
            assertEquals(lookupKey, result);
        }
    }
}
