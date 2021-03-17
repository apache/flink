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

import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Map;

/** JSON deserializer for {@link ResolvedCatalogTable}. */
public class CatalogTableJsonDeserializer extends StdDeserializer<ResolvedCatalogTable> {
    private static final long serialVersionUID = 1L;

    public CatalogTableJsonDeserializer() {
        super(ResolvedCatalogTable.class);
    }

    @Override
    public ResolvedCatalogTable deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        return deserialize(jsonParser, (FlinkDeserializationContext) ctx);
    }

    private ResolvedCatalogTable deserialize(JsonParser jsonParser, FlinkDeserializationContext ctx)
            throws IOException {
        final CatalogManager catalogManager =
                ctx.getSerdeContext().getFlinkContext().getCatalogManager();

        final Map<String, String> catalogProperties =
                jsonParser.readValueAs(new TypeReference<Map<String, String>>() {});

        final CatalogTable unresolvedTable = CatalogTable.fromProperties(catalogProperties);

        return (ResolvedCatalogTable) catalogManager.resolveCatalogBaseTable(unresolvedTable);
    }
}
