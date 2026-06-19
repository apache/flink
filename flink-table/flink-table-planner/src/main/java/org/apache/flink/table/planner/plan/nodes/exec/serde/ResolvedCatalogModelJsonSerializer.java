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
import org.apache.flink.table.catalog.ResolvedCatalogModel;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * JSON deserializer for {@link ResolvedCatalogModel}.
 *
 * @see ResolvedCatalogModelJsonDeserializer for the reverse operation
 */
@Internal
public class ResolvedCatalogModelJsonSerializer extends StdSerializer<ResolvedCatalogModel> {

    private static final long serialVersionUID = 1L;

    static final String SERIALIZE_OPTIONS = "serialize_options";

    static final String INPUT_SCHEMA = "inputSchema";
    static final String OUTPUT_SCHEMA = "outputSchema";
    static final String OPTIONS = "options";
    static final String COMMENT = "comment";

    public ResolvedCatalogModelJsonSerializer() {
        super(ResolvedCatalogModel.class);
    }

    @Override
    public void serialize(
            ResolvedCatalogModel model,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        boolean serializeOptions =
                serializerProvider.getAttribute(SERIALIZE_OPTIONS) == null
                        || (boolean) serializerProvider.getAttribute(SERIALIZE_OPTIONS);
        serialize(model, serializeOptions, jsonGenerator, serializerProvider);
    }

    static void serialize(
            ResolvedCatalogModel resolvedCatalogModel,
            boolean serializeOptions,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        serializerProvider.defaultSerializeField(
                INPUT_SCHEMA, resolvedCatalogModel.getResolvedInputSchema(), jsonGenerator);
        serializerProvider.defaultSerializeField(
                OUTPUT_SCHEMA, resolvedCatalogModel.getResolvedOutputSchema(), jsonGenerator);
        if (serializeOptions) {
            serializerProvider.defaultSerializeField(
                    OPTIONS, resolvedCatalogModel.getOptions(), jsonGenerator);
            if (resolvedCatalogModel.getComment() != null) {
                serializerProvider.defaultSerializeField(
                        COMMENT, resolvedCatalogModel.getComment(), jsonGenerator);
            }
        }

        jsonGenerator.writeEndObject();
    }
}
