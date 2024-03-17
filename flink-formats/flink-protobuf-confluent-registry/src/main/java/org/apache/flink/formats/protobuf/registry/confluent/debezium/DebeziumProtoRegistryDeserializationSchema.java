/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryDeserializationSchema;
import org.apache.flink.formats.protobuf.registry.confluent.ProtoToRowDataConverters;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static java.lang.String.format;

/** Check for type of fieldDesriptor == message */
public class DebeziumProtoRegistryDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;
    private static final String OP_READ = "r";
    /** insert operation. */
    private static final String OP_CREATE = "c";
    /** update operation. */
    private static final String OP_UPDATE = "u";
    /** delete operation. */
    private static final String OP_DELETE = "d";

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s message is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";
    private final ProtoRegistryDeserializationSchema protoDeserializer;
    private final transient RowType rowType;
    private transient Descriptors.Descriptor debeziumEnvelopDescriptor;
    private transient @Nullable Descriptors.FieldDescriptor descriptorForBefore;
    private transient @Nullable Descriptors.FieldDescriptor descriptorForAfter;

    private transient @Nullable ProtoToRowDataConverters.ProtoToRowDataConverter beforeConverter;
    private transient @Nullable ProtoToRowDataConverters.ProtoToRowDataConverter afterConverter;


    public DebeziumProtoRegistryDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> producedTypeInfo,
            SchemaRegistryConfig schemaRegistryConfig) {
        protoDeserializer =
                new ProtoRegistryDeserializationSchema(
                        schemaRegistryConfig, rowType, producedTypeInfo);
        this.rowType = rowType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protoDeserializer.open(context);
        this.debeziumEnvelopDescriptor = protoDeserializer.getMessageDescriptor();
        //do basic validation
        this.descriptorForBefore = debeziumEnvelopDescriptor.findFieldByName(EnvelopField.BEFORE);
        this.descriptorForAfter = debeziumEnvelopDescriptor.findFieldByName(EnvelopField.AFTER);

        //todo - need to assert presence
        beforeConverter = maybeCreateConverter(descriptorForBefore);
        afterConverter = maybeCreateConverter(descriptorForAfter);
    }


    private ProtoToRowDataConverters.ProtoToRowDataConverter maybeCreateConverter(
            @Nullable Descriptors.FieldDescriptor descriptor){
        ProtoToRowDataConverters.ProtoToRowDataConverter converter = null;

        if(descriptor!=null && descriptor.getMessageType()!=null)
            converter = ProtoToRowDataConverters.createConverter(
                this.descriptorForBefore.getMessageType(), rowType);

        return converter;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }


    private <T> Optional<T> extractPayload(
            DynamicMessage debeziumEnvelop, String field,
            ProtoToRowDataConverters.ProtoToRowDataConverter converter)
            throws IOException {

        T row = null;


        Descriptors.FieldDescriptor fieldDescriptor =
                debeziumEnvelopDescriptor.findFieldByName(field);
        if (Objects.nonNull(debeziumEnvelop.getField(fieldDescriptor))) {
            row =
                    (T)
                            converter.convert(
                                    (DynamicMessage) debeziumEnvelop.getField(
                                            fieldDescriptor));
        }
        return Optional.ofNullable(row);
    }



    void extractIfPresent(Optional<RowData> row, Collector<RowData> out, RowKind kind) {
        if (row.isPresent()) {
            RowData r = row.get();
            r.setRowKind(kind);
            out.collect(r);
        }
    }

    void extractOrElseThrow(Optional<RowData> row, Collector<RowData> out, RowKind kind) {
        if (!row.isPresent()) {
            throw new IllegalStateException(String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
        }
        extractIfPresent(row, out, kind);
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {

        DynamicMessage debeziumEnvelop = protoDeserializer.parseFrom(message);
        Optional<RowData> before =
                extractPayload(debeziumEnvelop, EnvelopField.BEFORE, beforeConverter );
        Optional<RowData> after =
                extractPayload(debeziumEnvelop, EnvelopField.AFTER,afterConverter );
        Optional<String> operation = extractPayload(debeziumEnvelop, EnvelopField.OP, Object::toString);

        if (!operation.isPresent()) {
            throw new IOException("Malformed debezium proto message");
        } else {
            String op = operation.get();

            if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
                extractIfPresent(after, out, RowKind.INSERT);
            } else if (OP_UPDATE.equals(op)) {
                extractOrElseThrow(before, out, RowKind.UPDATE_BEFORE);
                extractIfPresent(after, out, RowKind.UPDATE_AFTER);

            } else if (OP_DELETE.equals(op)) {
                extractOrElseThrow(before, out, RowKind.DELETE);

            } else {
                throw new IOException(
                        format(
                                "Unknown \"op\" value \"%s\". The Debezium Avro message is '%s'",
                                op, new String(message)));
            }
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    private static class EnvelopField {
        private static String BEFORE = "before";
        private static String AFTER = "after";
        private static String OP = "op";
    }
}
