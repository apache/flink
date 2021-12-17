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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.calcite.rex.RexWindowBound;

import java.io.IOException;

/**
 * JSON serializer for {@link RexWindowBound}. refer to {@link RexWindowBoundJsonDeserializer} for
 * deserializer.
 *
 * <p>Supports serialize CURRENT_ROW, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING, Preceding Bounded
 * Window and Following Bounded Window.
 */
public class RexWindowBoundJsonSerializer extends StdSerializer<RexWindowBound> {

    public static final String FIELD_NAME_KIND = "kind";
    public static final String KIND_CURRENT_ROW = "CURRENT_ROW";
    public static final String KIND_UNBOUNDED_PRECEDING = "UNBOUNDED_PRECEDING";
    public static final String KIND_UNBOUNDED_FOLLOWING = "UNBOUNDED_FOLLOWING";
    public static final String KIND_BOUNDED_WINDOW = "BOUNDED_WINDOW";

    public static final String FIELD_NAME_IS_PRECEDING = "isPreceding";
    public static final String FIELD_NAME_IS_FOLLOWING = "isFollowing";
    public static final String FIELD_NAME_OFFSET = "offset";

    public RexWindowBoundJsonSerializer() {
        super(RexWindowBound.class);
    }

    @Override
    public void serialize(
            RexWindowBound rexWindowBound, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        gen.writeStartObject();
        if (rexWindowBound.isCurrentRow()) {
            gen.writeStringField(FIELD_NAME_KIND, KIND_CURRENT_ROW);
        } else if (rexWindowBound.isUnbounded()) {
            if (rexWindowBound.isPreceding()) {
                gen.writeStringField(FIELD_NAME_KIND, KIND_UNBOUNDED_PRECEDING);
            } else if (rexWindowBound.isFollowing()) {
                gen.writeStringField(FIELD_NAME_KIND, KIND_UNBOUNDED_FOLLOWING);
            } else {
                throw new TableException("Unknown RexWindowBound: " + rexWindowBound);
            }
        } else {
            gen.writeStringField(FIELD_NAME_KIND, KIND_BOUNDED_WINDOW);
            if (rexWindowBound.isPreceding()) {
                gen.writeBooleanField(FIELD_NAME_IS_PRECEDING, true);
            } else if (rexWindowBound.isFollowing()) {
                gen.writeBooleanField(FIELD_NAME_IS_FOLLOWING, true);
            } else {
                throw new TableException("Unknown RexWindowBound: " + rexWindowBound);
            }
            gen.writeObjectField(FIELD_NAME_OFFSET, rexWindowBound.getOffset());
        }
        gen.writeEndObject();
    }
}
