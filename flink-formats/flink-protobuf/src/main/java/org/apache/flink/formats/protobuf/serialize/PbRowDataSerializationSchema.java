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

package org.apache.flink.formats.protobuf.serialize;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.formats.protobuf.PbSchemaValidator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.Descriptors;

/**
 * Serialization schema from Flink to Protobuf types.
 *
 * <p>Serializes a {@link RowData } to protobuf binary data.
 *
 * <p>Failures during deserialization are forwarded as wrapped {@link FlinkRuntimeException}.
 */
public class PbRowDataSerializationSchema implements SerializationSchema<RowData> {

    private final RowType rowType;

    private final String messageClassName;

    private transient RowToProtoConverter rowToProtoConverter;

    public PbRowDataSerializationSchema(RowType rowType, String messageClassName) {
        this.rowType = rowType;
        this.messageClassName = messageClassName;
        Descriptors.Descriptor descriptor = PbFormatUtils.getDescriptor(messageClassName);
        new PbSchemaValidator(descriptor, rowType).validate();
        try {
            // validate converter in client side to early detect errors
            rowToProtoConverter = new RowToProtoConverter(messageClassName, rowType);
        } catch (PbCodegenException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        rowToProtoConverter = new RowToProtoConverter(messageClassName, rowType);
    }

    @Override
    public byte[] serialize(RowData element) {
        try {
            return rowToProtoConverter.convertRowToProtoBinary(element);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
