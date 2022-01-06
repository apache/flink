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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/** JSON serializer for {@link ContextResolvedTable}. */
class ContextResolvedTableJsonSerializer extends StdSerializer<ContextResolvedTable> {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_IDENTIFIER = "identifier";
    public static final String FIELD_NAME_CATALOG_TABLE = "resolvedTable";

    public ContextResolvedTableJsonSerializer() {
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

        if (contextResolvedTable.isAnonymous()
                && planCompilationOption == CatalogPlanCompilation.IDENTIFIER) {
            throw cannotSerializeAnonymousTable(contextResolvedTable.getIdentifier());
        }

        jsonGenerator.writeStartObject();

        if (!contextResolvedTable.isAnonymous()) {
            // Serialize object identifier
            jsonGenerator.writeObjectField(
                    FIELD_NAME_IDENTIFIER, contextResolvedTable.getIdentifier());
        }

        if ((contextResolvedTable.isPermanent() || contextResolvedTable.isAnonymous())
                && planCompilationOption != CatalogPlanCompilation.IDENTIFIER) {
            jsonGenerator.writeFieldName(FIELD_NAME_CATALOG_TABLE);
            ResolvedCatalogTableJsonSerializer.serialize(
                    contextResolvedTable.getResolvedTable(),
                    planCompilationOption == CatalogPlanCompilation.ALL,
                    jsonGenerator,
                    serializerProvider);
        }

        jsonGenerator.writeEndObject();
    }

    static ValidationException cannotSerializeAnonymousTable(ObjectIdentifier objectIdentifier) {
        return new ValidationException(
                String.format(
                        "Cannot serialize anonymous table '%s', as the option '%s' == '%s' forces to serialize only the table identifier, "
                                + "but the anonymous table identifier cannot be serialized. "
                                + "Either modify the table to be temporary or permanent, or regenerate the plan with '%s' != '%s'.",
                        objectIdentifier,
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS.key(),
                        CatalogPlanCompilation.IDENTIFIER.name(),
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS.key(),
                        CatalogPlanCompilation.IDENTIFIER.name()));
    }
}
