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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.formats.protobuf.PbSchemaValidator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from Protobuf to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a protobuf object and reads the specified
 * fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
public class PbRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;

    private final String messageClassName;
    private final boolean ignoreParseErrors;
    private final boolean readDefaultValues;

    private transient ProtoToRowConverter protoToRowConverter;

    public PbRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            String messageClassName,
            boolean ignoreParseErrors,
            boolean readDefaultValues) {
        checkNotNull(rowType, "rowType cannot be null");
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.messageClassName = messageClassName;
        this.ignoreParseErrors = ignoreParseErrors;
        this.readDefaultValues = readDefaultValues;
        // do it in client side to report error in the first place
        new PbSchemaValidator(PbFormatUtils.getDescriptor(messageClassName), rowType).validate();
        // this step is only used to validate codegen in client side in the first place
        try {
            // validate converter in client side to early detect errors
            protoToRowConverter =
                    new ProtoToRowConverter(messageClassName, rowType, readDefaultValues);
        } catch (PbCodegenException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protoToRowConverter = new ProtoToRowConverter(messageClassName, rowType, readDefaultValues);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            return protoToRowConverter.convertProtoBinaryToRow(message);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException("Failed to deserialize PB object.", t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PbRowDataDeserializationSchema that = (PbRowDataDeserializationSchema) o;
        return ignoreParseErrors == that.ignoreParseErrors
                && readDefaultValues == that.readDefaultValues
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(resultTypeInfo, that.resultTypeInfo)
                && Objects.equals(messageClassName, that.messageClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rowType, resultTypeInfo, messageClassName, ignoreParseErrors, readDefaultValues);
    }
}
