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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.calcite.rel.core.AggregateCall;

import java.io.IOException;

/**
 * JSON serializer for {@link AggregateCall}.
 *
 * @see AggregateCallJsonDeserializer for the reverse operation
 */
@Internal
final class AggregateCallJsonSerializer extends StdSerializer<AggregateCall> {
    private static final long serialVersionUID = 1L;

    static final String FIELD_NAME_NAME = "name";
    static final String FIELD_NAME_ARG_LIST = "argList";
    static final String FIELD_NAME_FILTER_ARG = "filterArg";
    static final String FIELD_NAME_DISTINCT = "distinct";
    static final String FIELD_NAME_APPROXIMATE = "approximate";
    static final String FIELD_NAME_IGNORE_NULLS = "ignoreNulls";
    static final String FIELD_NAME_TYPE = "type";

    AggregateCallJsonSerializer() {
        super(AggregateCall.class);
    }

    @Override
    public void serialize(
            AggregateCall aggCall,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        final ReadableConfig config = SerdeContext.get(serializerProvider).getConfiguration();
        final CatalogPlanCompilation compilationStrategy =
                config.get(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS);

        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_NAME, aggCall.getName());
        RexNodeJsonSerializer.serializeSqlOperator(
                aggCall.getAggregation(),
                jsonGenerator,
                serializerProvider,
                compilationStrategy == CatalogPlanCompilation.ALL);
        jsonGenerator.writeFieldName(FIELD_NAME_ARG_LIST);
        jsonGenerator.writeStartArray();
        for (int arg : aggCall.getArgList()) {
            jsonGenerator.writeNumber(arg);
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeNumberField(FIELD_NAME_FILTER_ARG, aggCall.filterArg);
        jsonGenerator.writeBooleanField(FIELD_NAME_DISTINCT, aggCall.isDistinct());
        jsonGenerator.writeBooleanField(FIELD_NAME_APPROXIMATE, aggCall.isApproximate());
        jsonGenerator.writeBooleanField(FIELD_NAME_IGNORE_NULLS, aggCall.ignoreNulls());
        serializerProvider.defaultSerializeField(FIELD_NAME_TYPE, aggCall.getType(), jsonGenerator);
        jsonGenerator.writeEndObject();
    }
}
