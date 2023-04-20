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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.calcite.rel.type.RelDataType;

import java.io.IOException;

/**
 * JSON serializer for {@link RelDataType}.
 *
 * @see RelDataTypeJsonDeserializer for the reverse operation.
 */
@Internal
final class RelDataTypeJsonSerializer extends StdSerializer<RelDataType> {
    private static final long serialVersionUID = 1L;

    RelDataTypeJsonSerializer() {
        super(RelDataType.class);
    }

    @Override
    public void serialize(
            RelDataType relDataType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        final SerdeContext serdeContext = SerdeContext.get(serializerProvider);
        final DataTypeFactory dataTypeFactory =
                serdeContext.getFlinkContext().getCatalogManager().getDataTypeFactory();
        // Conversion to LogicalType also ensures that Calcite's type system is materialized
        // so data types like DECIMAL will receive a concrete precision and scale (not unspecified
        // anymore).
        final LogicalType logicalType =
                LogicalRelDataTypeConverter.toLogicalType(relDataType, dataTypeFactory);
        serializerProvider.defaultSerializeValue(logicalType, jsonGenerator);
    }
}
