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
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;

/** Mocks and utilities for serde tests. */
final class JsonSerdeMocks {

    private JsonSerdeMocks() {
        // no instantiation
    }

    static SerdeContext configuredSerdeContext() {
        return configuredSerdeContext(
                CatalogManagerMocks.createEmptyCatalogManager(), TableConfig.getDefault());
    }

    static SerdeContext configuredSerdeContext(
            CatalogManager catalogManager, TableConfig tableConfig) {
        return new SerdeContext(
                new FlinkContextImpl(
                        false, tableConfig, new ModuleManager(), null, catalogManager, null),
                Thread.currentThread().getContextClassLoader(),
                FlinkTypeFactory.INSTANCE(),
                FlinkSqlOperatorTable.instance());
    }

    static String toJson(SerdeContext serdeContext, Object object) {
        final ObjectWriter objectWriter = JsonSerdeUtil.createObjectWriter(serdeContext);
        final String json;
        try {
            json = objectWriter.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
        return json;
    }

    static <T> T toObject(SerdeContext serdeContext, String json, Class<T> clazz) {
        final ObjectReader objectReader = JsonSerdeUtil.createObjectReader(serdeContext);
        try {
            return objectReader.readValue(json, clazz);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
