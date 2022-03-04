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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.catalog.ContextResolvedTable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * JSON serializer for {@link ContextResolvedTable}.
 *
 * @see ContextResolvedTableJsonDeserializer for the reverse operation
 */
@Internal
final class ContextResolvedTableJsonSerializer extends StdSerializer<ContextResolvedTable> {
    private static final long serialVersionUID = 1L;

    static final String FIELD_NAME_IDENTIFIER = "identifier";
    static final String FIELD_NAME_CATALOG_TABLE = "resolvedTable";

    ContextResolvedTableJsonSerializer() {
        super(ContextResolvedTable.class);
    }

    @Override
    public void serialize(
            ContextResolvedTable contextResolvedTable,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        final CatalogPlanCompilation planCompilationOption =
                SerdeContext.get(serializerProvider)
                        .getConfiguration()
                        .get(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS);

        jsonGenerator.writeStartObject();

        if (contextResolvedTable.isAnonymous()) {
            // For anonymous tables, we always write everything
            jsonGenerator.writeFieldName(FIELD_NAME_CATALOG_TABLE);
            ResolvedCatalogTableJsonSerializer.serialize(
                    contextResolvedTable.getResolvedTable(),
                    true,
                    jsonGenerator,
                    serializerProvider);
        } else {
            // Serialize object identifier
            jsonGenerator.writeObjectField(
                    FIELD_NAME_IDENTIFIER, contextResolvedTable.getIdentifier());
            if (contextResolvedTable.isPermanent()
                    && planCompilationOption != CatalogPlanCompilation.IDENTIFIER) {
                jsonGenerator.writeFieldName(FIELD_NAME_CATALOG_TABLE);
                try {
                    ResolvedCatalogTableJsonSerializer.serialize(
                            contextResolvedTable.getResolvedTable(),
                            planCompilationOption == CatalogPlanCompilation.ALL,
                            jsonGenerator,
                            serializerProvider);
                } catch (ValidationException e) {
                    throw new ValidationException(
                            String.format(
                                    "Error when trying to serialize table '%s'.",
                                    contextResolvedTable.getIdentifier()),
                            e);
                }
            }
        }

        jsonGenerator.writeEndObject();
    }
}
