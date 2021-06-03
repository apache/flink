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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Map;

/** JSON serializer for {@link ResolvedCatalogTable}. */
public class CatalogTableJsonSerializer extends StdSerializer<ResolvedCatalogTable> {
    private static final long serialVersionUID = 1L;

    public CatalogTableJsonSerializer() {
        super(ResolvedCatalogTable.class);
    }

    @Override
    public void serialize(
            ResolvedCatalogTable catalogTable,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        final Map<String, String> properties;
        try {
            properties = catalogTable.toProperties();
        } catch (Exception e) {
            throw new TableException("Unable to serialize catalog table: " + catalogTable, e);
        }

        jsonGenerator.writeStartObject();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            jsonGenerator.writeStringField(entry.getKey(), entry.getValue());
        }
        jsonGenerator.writeEndObject();
    }
}
