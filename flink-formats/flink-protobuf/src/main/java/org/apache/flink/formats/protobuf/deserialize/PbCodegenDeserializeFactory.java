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

package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

import static org.apache.flink.formats.protobuf.util.PbFormatUtils.isArrayType;

/** Codegen factory class which return {@link PbCodegenDeserializer} of different data type. */
public class PbCodegenDeserializeFactory {
    public static PbCodegenDeserializer getPbCodegenDes(
            Descriptors.FieldDescriptor fd, LogicalType type, PbFormatContext formatContext)
            throws PbCodegenException {
        return getPbCodegenDes(fd, type, formatContext, false);
    }

    public static PbCodegenDeserializer getPbCodegenDes(
            Descriptors.FieldDescriptor fd,
            LogicalType type,
            PbFormatContext formatContext,
            boolean withinRepeated)
            throws PbCodegenException {
        // We need to revert to FieldDescriptor to check as we only have the final LogicalType
        // can not infer the intermediate logical type.
        if (!fd.isRepeated() || withinRepeated) {
            if (fd.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
                return new PbCodegenRowDeserializer(fd.getMessageType(), type, formatContext);
            } else {
                return new PbCodegenSimpleDeserializer(fd, type);
            }
        } else {
            // ArrayType and MapType does not contain nested field, just pass in the final row type
            if (isArrayType(fd)) {
                return new PbCodegenArrayDeserializer(
                        fd, ((ArrayType) type).getElementType(), formatContext);
            } else if (fd.isMapField()) {
                return new PbCodegenMapDeserializer(fd, (MapType) type, formatContext);
            } else {
                throw new PbCodegenException("Do not support flink type: " + type);
            }
        }
    }

    public static PbCodegenDeserializer getPbCodegenTopRowDes(
            Descriptors.Descriptor descriptor,
            RowType rowType,
            PbFormatContext formatContext,
            String[][] projectedFields) {
        return new PbCodegenTopRowDeserializer(descriptor, rowType, formatContext, projectedFields);
    }
}
