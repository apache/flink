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
import org.apache.flink.table.catalog.ContextResolvedModel;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * Serializer for {@link ContextResolvedModel}.
 *
 * @see ContextResolvedModelJsonDeserializer for the reverse operation
 */
@Internal
public class ContextResolvedModelJsonSerializer extends StdSerializer<ContextResolvedModel> {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_IDENTIFIER = "identifier";
    public static final String FIELD_NAME_CATALOG_MODEL = "resolvedModel";

    public ContextResolvedModelJsonSerializer() {
        super(ContextResolvedModel.class);
    }

    @Override
    public void serialize(
            ContextResolvedModel contextResolvedModel,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        final TableConfigOptions.CatalogPlanCompilation planCompilationOption =
                SerdeContext.get(serializerProvider)
                        .getConfiguration()
                        .get(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS);
        jsonGenerator.writeStartObject();

        // Serialize object identifier
        jsonGenerator.writeObjectField(FIELD_NAME_IDENTIFIER, contextResolvedModel.getIdentifier());

        if ((contextResolvedModel.isPermanent())
                && planCompilationOption != TableConfigOptions.CatalogPlanCompilation.IDENTIFIER) {
            try {
                jsonGenerator.writeFieldName(FIELD_NAME_CATALOG_MODEL);
                ResolvedCatalogModelJsonSerializer.serialize(
                        contextResolvedModel.getResolvedModel(),
                        planCompilationOption == TableConfigOptions.CatalogPlanCompilation.ALL,
                        jsonGenerator,
                        serializerProvider);
            } catch (ValidationException e) {
                throw new ValidationException(
                        String.format(
                                "Error when trying to serialize model '%s'.",
                                contextResolvedModel.getIdentifier()),
                        e);
            }
        }

        jsonGenerator.writeEndObject();
    }
}
