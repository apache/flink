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
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.expressions.RexNodeExpression;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * JSON serializer for {@link ResolvedExpression}.
 *
 * @see ResolvedExpressionJsonDeserializer for the reverse operation
 */
@Internal
final class ResolvedExpressionJsonSerializer extends StdSerializer<ResolvedExpression> {

    static final String TYPE = "type";
    static final String TYPE_REX_NODE_EXPRESSION = "rexNodeExpression";
    static final String REX_NODE = "rexNode";
    static final String SERIALIZABLE_STRING = "serializableString";

    ResolvedExpressionJsonSerializer() {
        super(ResolvedExpression.class);
    }

    @Override
    public void serialize(
            ResolvedExpression resolvedExpression,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        if (resolvedExpression instanceof RexNodeExpression) {
            serialize((RexNodeExpression) resolvedExpression, jsonGenerator, serializerProvider);
        } else {
            throw new ValidationException(
                    String.format(
                            "Expression '%s' cannot be serialized. "
                                    + "Currently, only SQL expressions can be serialized in the persisted plan.",
                            resolvedExpression.asSummaryString()));
        }

        jsonGenerator.writeEndObject();
    }

    private void serialize(
            RexNodeExpression expression,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        serializerProvider.defaultSerializeField(REX_NODE, expression.getRexNode(), jsonGenerator);
        jsonGenerator.writeStringField(SERIALIZABLE_STRING, expression.asSerializableString());
    }
}
