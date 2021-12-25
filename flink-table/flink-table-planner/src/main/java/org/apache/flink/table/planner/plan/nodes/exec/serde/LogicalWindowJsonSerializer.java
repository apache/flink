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
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.types.AtomicDataType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.table.planner.plan.utils.AggregateUtil.hasTimeIntervalType;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.toDuration;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.toLong;

/**
 * JSON serializer for {@link LogicalWindow}, refer to {@link LogicalWindowJsonDeserializer} for
 * deserializer.
 */
public class LogicalWindowJsonSerializer extends StdSerializer<LogicalWindow> {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_KIND = "kind";
    public static final String KIND_TUMBLING = "TUMBLING";
    public static final String KIND_SLIDING = "SLIDING";
    public static final String KIND_SESSION = "SESSION";

    public static final String FIELD_NAME_ALIAS = "alias";
    public static final String FIELD_NAME_TIME_FIELD = "timeField";
    public static final String FIELD_NAME_FIELD_NAME = "fieldName";
    public static final String FIELD_NAME_FIELD_INDEX = "fieldIndex";
    public static final String FIELD_NAME_INPUT_INDEX = "inputIndex";
    public static final String FIELD_NAME_FIELD_TYPE = "fieldType";

    public static final String FIELD_NAME_SIZE = "size";
    public static final String FIELD_NAME_IS_TIME_WINDOW = "isTimeWindow";

    public static final String FIELD_NAME_SLIDE = "slide";
    public static final String FIELD_NAME_GAP = "gap";

    public LogicalWindowJsonSerializer() {
        super(LogicalWindow.class);
    }

    @Override
    public void serialize(
            LogicalWindow logicalWindow, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        gen.writeStartObject();
        if (logicalWindow instanceof TumblingGroupWindow) {
            TumblingGroupWindow window = (TumblingGroupWindow) logicalWindow;
            gen.writeStringField(FIELD_NAME_KIND, KIND_TUMBLING);
            gen.writeObjectField(FIELD_NAME_ALIAS, logicalWindow.aliasAttribute());
            FieldReferenceExpression timeField = logicalWindow.timeAttribute();
            serializeFieldReferenceExpression(timeField, gen);
            ValueLiteralExpression size = window.size();
            if (hasTimeIntervalType(size)) {
                Duration duration = toDuration(size);
                gen.writeBooleanField(FIELD_NAME_IS_TIME_WINDOW, true);
                gen.writeObjectField(FIELD_NAME_SIZE, duration);
            } else {
                Long duration = toLong(size);
                gen.writeBooleanField(FIELD_NAME_IS_TIME_WINDOW, false);
                gen.writeNumberField(FIELD_NAME_SIZE, duration);
            }
        } else if (logicalWindow instanceof SlidingGroupWindow) {
            SlidingGroupWindow window = (SlidingGroupWindow) logicalWindow;
            gen.writeStringField(FIELD_NAME_KIND, KIND_SLIDING);
            gen.writeObjectField(FIELD_NAME_ALIAS, window.aliasAttribute());
            serializeFieldReferenceExpression(window.timeAttribute(), gen);

            ValueLiteralExpression size = window.size();
            if (hasTimeIntervalType(size)) {
                Duration duration = toDuration(size);
                gen.writeBooleanField(FIELD_NAME_IS_TIME_WINDOW, true);
                gen.writeObjectField(FIELD_NAME_SIZE, duration);
                gen.writeObjectField(FIELD_NAME_SLIDE, toDuration(window.slide()));
            } else {
                Long duration = toLong(size);
                gen.writeBooleanField(FIELD_NAME_IS_TIME_WINDOW, false);
                gen.writeObjectField(FIELD_NAME_SIZE, duration);
                gen.writeObjectField(FIELD_NAME_SLIDE, toLong(window.slide()));
            }

        } else if (logicalWindow instanceof SessionGroupWindow) {
            gen.writeStringField(FIELD_NAME_KIND, KIND_SESSION);
            SessionGroupWindow window = (SessionGroupWindow) logicalWindow;
            gen.writeObjectField(FIELD_NAME_ALIAS, window.aliasAttribute());
            serializeFieldReferenceExpression(window.timeAttribute(), gen);
            gen.writeObjectField(FIELD_NAME_GAP, toDuration(window.gap()));
        } else {
            throw new TableException("Unknown LogicalWindow: " + logicalWindow);
        }
        gen.writeEndObject();
    }

    private void serializeFieldReferenceExpression(
            FieldReferenceExpression timeField, JsonGenerator gen) throws IOException {
        gen.writeObjectFieldStart(FIELD_NAME_TIME_FIELD);
        gen.writeStringField(FIELD_NAME_FIELD_NAME, timeField.getName());
        gen.writeNumberField(FIELD_NAME_FIELD_INDEX, timeField.getFieldIndex());
        gen.writeNumberField(FIELD_NAME_INPUT_INDEX, timeField.getInputIndex());
        if (timeField.getOutputDataType() instanceof AtomicDataType) {
            gen.writeObjectField(
                    FIELD_NAME_FIELD_TYPE, timeField.getOutputDataType().getLogicalType());
        } else {
            throw new TableException("Unknown TimeField in LogicalWindow: " + timeField);
        }
        gen.writeEndObject();
    }
}
