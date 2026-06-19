/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.protobuf;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_ITEM_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_KEY_VALUE_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_PREFIX;

/**
 * Implementation of {@link WriteSupport} for writing Protocol Buffers.
 *
 * <p>NOTE: This is a vendored patched version of ProtoWriteSupport to work with protobuf 4.x. The
 * patch replaces the deprecated/removed enum based syntax detection with a string based approach
 * compatible with protobuf 3 and 4.See parquet-java issue
 * https://github.com/apache/parquet-java/issues/3175.
 *
 * <p>The original source can be found here:
 * https://github.com/apache/parquet-java/blob/apache-parquet-1.15.2/parquet-protobuf/src/main/java/org/apache/parquet/proto/ProtoWriteSupport.java
 *
 * <p>Patched code is marked with BEGIN PATCH / END PATCH comments in the source.
 */
class PatchedProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PatchedProtoWriteSupport.class);
    public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
    // PARQUET-968 introduces changes to allow writing specs compliant schemas with
    // parquet-protobuf.
    // In the past, collection were not written using the LIST and MAP wrappers and thus were not
    // compliant
    // with the parquet specs. This flag, is set to true, allows to write using spec compliant
    // schemas
    // but is set to false by default to keep backward compatibility.
    public static final String PB_SPECS_COMPLIANT_WRITE = "parquet.proto.writeSpecsCompliant";

    public static final String PB_UNWRAP_PROTO_WRAPPERS = "parquet.proto.unwrapProtoWrappers";

    private boolean writeSpecsCompliant = false;
    private boolean unwrapProtoWrappers = false;
    private RecordConsumer recordConsumer;
    private Class<? extends Message> protoMessage;
    private Descriptor descriptor;
    private MessageWriter messageWriter;
    // Keep protobuf enum value with number in the metadata, so that in read time, a reader can read
    // at least
    // the number back even with an outdated schema which might not contain all enum values.
    private Map<String, Map<String, Integer>> protoEnumBookKeeper = new HashMap<>();

    public PatchedProtoWriteSupport() {}

    public PatchedProtoWriteSupport(Class<? extends Message> protobufClass) {
        this.protoMessage = protobufClass;
    }

    public PatchedProtoWriteSupport(Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public String getName() {
        return "protobuf";
    }

    public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
        configuration.setClass(PB_CLASS_WRITE, protoClass, Message.class);
    }

    /**
     * Make parquet-protobuf use the LIST and MAP wrappers for collections. Set to false if you need
     * backward compatibility with parquet before PARQUET-968 (1.9.0 and older).
     *
     * @param configuration The hadoop configuration
     * @param writeSpecsCompliant If set to true, the old schema style will be used (without
     *     wrappers).
     */
    public static void setWriteSpecsCompliant(
            Configuration configuration, boolean writeSpecsCompliant) {
        configuration.setBoolean(PB_SPECS_COMPLIANT_WRITE, writeSpecsCompliant);
    }

    public static void setUnwrapProtoWrappers(
            Configuration configuration, boolean unwrapProtoWrappers) {
        configuration.setBoolean(PB_UNWRAP_PROTO_WRAPPERS, unwrapProtoWrappers);
    }

    /**
     * Writes Protocol buffer to parquet file.
     *
     * @param record instance of Message.Builder or Message.
     */
    @Override
    public void write(T record) {
        recordConsumer.startMessage();
        try {
            messageWriter.writeTopLevelMessage(record);
        } catch (RuntimeException e) {
            Message m =
                    (record instanceof Message.Builder)
                            ? ((Message.Builder) record).build()
                            : (Message) record;
            LOG.error("Cannot write message {}: {}", e.getMessage(), m);
            throw e;
        }
        recordConsumer.endMessage();
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return init(new HadoopParquetConfiguration(configuration));
    }

    @Override
    public WriteContext init(ParquetConfiguration configuration) {

        Map<String, String> extraMetaData = new HashMap<>();

        // if no protobuf descriptor was given in constructor, load descriptor from configuration
        // (set with
        // setProtobufClass)
        if (descriptor == null) {
            if (protoMessage == null) {
                Class<? extends Message> pbClass =
                        configuration.getClass(PB_CLASS_WRITE, null, Message.class);
                if (pbClass != null) {
                    protoMessage = pbClass;
                } else {
                    String msg = "Protocol buffer class or descriptor not specified.";
                    String hint =
                            " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
                    throw new BadConfigurationException(msg + hint);
                }
            }
            descriptor = Protobufs.getMessageDescriptor(protoMessage);
            extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
        }

        unwrapProtoWrappers =
                configuration.getBoolean(PB_UNWRAP_PROTO_WRAPPERS, unwrapProtoWrappers);
        writeSpecsCompliant =
                configuration.getBoolean(PB_SPECS_COMPLIANT_WRITE, writeSpecsCompliant);
        MessageType rootSchema = new PatchedProtoSchemaConverter(configuration).convert(descriptor);
        validatedMapping(descriptor, rootSchema);

        this.messageWriter = new MessageWriter(descriptor, rootSchema);

        extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, descriptor.toProto().toString());
        extraMetaData.put(PB_SPECS_COMPLIANT_WRITE, String.valueOf(writeSpecsCompliant));
        extraMetaData.put(PB_UNWRAP_PROTO_WRAPPERS, String.valueOf(unwrapProtoWrappers));
        return new WriteContext(rootSchema, extraMetaData);
    }

    @Override
    public FinalizedWriteContext finalizeWrite() {
        Map<String, String> protoMetadata = enumMetadata();
        return new FinalizedWriteContext(protoMetadata);
    }

    private Map<String, String> enumMetadata() {
        Map<String, String> enumMetadata = new HashMap<>();
        for (Map.Entry<String, Map<String, Integer>> enumNameNumberMapping :
                protoEnumBookKeeper.entrySet()) {
            StringBuilder nameNumberPairs = new StringBuilder();
            if (enumNameNumberMapping.getValue().isEmpty()) {
                // No enum is ever written to any column of this file, put an empty string as the
                // value in the metadata
                LOG.info("No enum is written for {}", enumNameNumberMapping.getKey());
            }
            int idx = 0;
            for (Map.Entry<String, Integer> nameNumberPair :
                    enumNameNumberMapping.getValue().entrySet()) {
                nameNumberPairs
                        .append(nameNumberPair.getKey())
                        .append(METADATA_ENUM_KEY_VALUE_SEPARATOR)
                        .append(nameNumberPair.getValue());
                idx++;
                if (idx < enumNameNumberMapping.getValue().size()) {
                    nameNumberPairs.append(METADATA_ENUM_ITEM_SEPARATOR);
                }
            }
            enumMetadata.put(
                    METADATA_ENUM_PREFIX + enumNameNumberMapping.getKey(),
                    nameNumberPairs.toString());
        }
        return enumMetadata;
    }

    class FieldWriter {
        String fieldName;
        int index = -1;

        void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        /** Sets index of field inside parquet message. */
        void setIndex(int index) {
            this.index = index;
        }

        /** Used for writing repeated fields. */
        void writeRawValue(Object value) {}

        /** Used for writing nonrepeated (optional, required) fields. */
        void writeField(Object value) {
            if (!(this instanceof PatchedProtoWriteSupport.MapWriter)) {
                recordConsumer.startField(fieldName, index);
            }
            writeRawValue(value);
            if (!(this instanceof PatchedProtoWriteSupport.MapWriter)) {
                recordConsumer.endField(fieldName, index);
            }
        }
    }

    class MessageWriter extends FieldWriter {

        final FieldWriter[] fieldWriters;

        @SuppressWarnings("unchecked")
        MessageWriter(Descriptor descriptor, GroupType schema) {
            List<FieldDescriptor> fields = descriptor.getFields();
            fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.size());

            for (FieldDescriptor fieldDescriptor : fields) {
                String name = fieldDescriptor.getName();
                Type type = schema.getType(name);
                FieldWriter writer = createWriter(fieldDescriptor, type);

                if (writeSpecsCompliant
                        && fieldDescriptor.isRepeated()
                        && !fieldDescriptor.isMapField()) {
                    writer = new ArrayWriter(writer);
                } else if (!writeSpecsCompliant && fieldDescriptor.isRepeated()) {
                    // the old schemas style used to write maps as repeated fields instead of
                    // wrapping them in a LIST
                    writer = new RepeatedWriter(writer);
                }

                writer.setFieldName(name);
                writer.setIndex(schema.getFieldIndex(name));

                fieldWriters[fieldDescriptor.getIndex()] = writer;
            }
        }

        private FieldWriter createWriter(FieldDescriptor fieldDescriptor, Type type) {

            switch (fieldDescriptor.getJavaType()) {
                case STRING:
                    return new StringWriter();
                case MESSAGE:
                    return createMessageWriter(fieldDescriptor, type);
                case INT:
                    return new IntWriter();
                case LONG:
                    return new LongWriter();
                case FLOAT:
                    return new FloatWriter();
                case DOUBLE:
                    return new DoubleWriter();
                case ENUM:
                    return new EnumWriter(fieldDescriptor.getEnumType());
                case BOOLEAN:
                    return new BooleanWriter();
                case BYTE_STRING:
                    return new BinaryWriter();
            }

            return unknownType(fieldDescriptor); // should not be executed, always throws exception.
        }

        private FieldWriter createMessageWriter(FieldDescriptor fieldDescriptor, Type type) {
            if (fieldDescriptor.isMapField() && writeSpecsCompliant) {
                return createMapWriter(fieldDescriptor, type);
            }

            if (unwrapProtoWrappers) {
                Descriptor messageType = fieldDescriptor.getMessageType();
                if (messageType.equals(Timestamp.getDescriptor())) {
                    return new TimestampWriter();
                }
                if (messageType.equals(Date.getDescriptor())) {
                    return new DateWriter();
                }
                if (messageType.equals(TimeOfDay.getDescriptor())) {
                    return new TimeWriter();
                }
                if (messageType.equals(DoubleValue.getDescriptor())) {
                    return new DoubleValueWriter();
                }
                if (messageType.equals(FloatValue.getDescriptor())) {
                    return new FloatValueWriter();
                }
                if (messageType.equals(Int64Value.getDescriptor())) {
                    return new Int64ValueWriter();
                }
                if (messageType.equals(UInt64Value.getDescriptor())) {
                    return new UInt64ValueWriter();
                }
                if (messageType.equals(Int32Value.getDescriptor())) {
                    return new Int32ValueWriter();
                }
                if (messageType.equals(UInt32Value.getDescriptor())) {
                    return new UInt32ValueWriter();
                }
                if (messageType.equals(BoolValue.getDescriptor())) {
                    return new BoolValueWriter();
                }
                if (messageType.equals(StringValue.getDescriptor())) {
                    return new StringValueWriter();
                }
                if (messageType.equals(BytesValue.getDescriptor())) {
                    return new BytesValueWriter();
                }
            }

            // This can happen now that recursive schemas get truncated to bytes.  Write the bytes.
            if (type.isPrimitive()
                    && type.asPrimitiveType().getPrimitiveTypeName()
                            == PrimitiveType.PrimitiveTypeName.BINARY) {
                return new BinaryWriter();
            }

            return new MessageWriter(fieldDescriptor.getMessageType(), getGroupType(type));
        }

        private GroupType getGroupType(Type type) {
            LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
                return type.asGroupType();
            }
            return logicalTypeAnnotation
                    .accept(
                            new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<GroupType>() {
                                @Override
                                public Optional<GroupType> visit(
                                        LogicalTypeAnnotation.ListLogicalTypeAnnotation
                                                listLogicalType) {
                                    return ofNullable(
                                            type.asGroupType()
                                                    .getType("list")
                                                    .asGroupType()
                                                    .getType("element")
                                                    .asGroupType());
                                }

                                @Override
                                public Optional<GroupType> visit(
                                        LogicalTypeAnnotation.MapLogicalTypeAnnotation
                                                mapLogicalType) {
                                    return ofNullable(
                                            type.asGroupType()
                                                    .getType("key_value")
                                                    .asGroupType()
                                                    .getType("value")
                                                    .asGroupType());
                                }
                            })
                    .orElse(type.asGroupType());
        }

        private MapWriter createMapWriter(FieldDescriptor fieldDescriptor, Type type) {
            List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
            if (fields.size() != 2) {
                throw new UnsupportedOperationException(
                        "Expected two fields for the map (key/value), but got: " + fields);
            }

            // KeyFieldWriter
            FieldDescriptor keyProtoField = fields.get(0);
            FieldWriter keyWriter = createWriter(keyProtoField, type);
            keyWriter.setFieldName(keyProtoField.getName());
            keyWriter.setIndex(0);

            // ValueFieldWriter
            FieldDescriptor valueProtoField = fields.get(1);
            FieldWriter valueWriter = createWriter(valueProtoField, type);
            valueWriter.setFieldName(valueProtoField.getName());
            valueWriter.setIndex(1);

            return new MapWriter(keyWriter, valueWriter);
        }

        /** Writes top level message. It cannot call startGroup() */
        void writeTopLevelMessage(Object value) {
            writeAllFields((MessageOrBuilder) value);
        }

        /** Writes message as part of repeated field. It cannot start field */
        @Override
        final void writeRawValue(Object value) {
            recordConsumer.startGroup();
            writeAllFields((MessageOrBuilder) value);
            recordConsumer.endGroup();
        }

        /** Used for writing nonrepeated (optional, required) fields. */
        @Override
        final void writeField(Object value) {
            recordConsumer.startField(fieldName, index);
            writeRawValue(value);
            recordConsumer.endField(fieldName, index);
        }

        private void writeAllFields(MessageOrBuilder pb) {
            Descriptor messageDescriptor = pb.getDescriptorForType();
            // ============================================================================
            // BEGIN PATCH: Replace enum-based syntax detection with string-based approach
            // ============================================================================
            String syntax = messageDescriptor.getFile().toProto().getSyntax();

            // Check for editions syntax (not supported)
            if ("editions".equals(syntax)) {
                throw new UnsupportedOperationException(
                        "Protocol Buffers editions syntax is not supported");
            }

            // proto2 uses empty string or "proto2", proto3 uses "proto3"
            boolean isProto2 = syntax.isEmpty() || "proto2".equals(syntax);

            if (isProto2) {
                // ============================================================================
                // END PATCH
                // ============================================================================
                // Returns changed fields with values. Map is ordered by id.
                Map<FieldDescriptor, Object> changedPbFields = pb.getAllFields();

                for (Map.Entry<FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
                    FieldDescriptor fieldDescriptor = entry.getKey();

                    if (fieldDescriptor.isExtension()) {
                        // Field index of an extension field might overlap with a base field.
                        throw new UnsupportedOperationException(
                                "Cannot convert Protobuf message with extension field(s)");
                    }

                    int fieldIndex = fieldDescriptor.getIndex();
                    fieldWriters[fieldIndex].writeField(entry.getValue());
                }
            } else {
                // proto3
                List<FieldDescriptor> fieldDescriptors = messageDescriptor.getFields();
                for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
                    FieldDescriptor.Type type = fieldDescriptor.getType();

                    // For a field in a oneOf that isn't set don't write anything
                    if (fieldDescriptor.getContainingOneof() != null
                            && !pb.hasField(fieldDescriptor)) {
                        continue;
                    }

                    if (!fieldDescriptor.isRepeated()
                            && FieldDescriptor.Type.MESSAGE.equals(type)
                            && !pb.hasField(fieldDescriptor)) {
                        continue;
                    }
                    int fieldIndex = fieldDescriptor.getIndex();
                    FieldWriter fieldWriter = fieldWriters[fieldIndex];
                    fieldWriter.writeField(pb.getField(fieldDescriptor));
                }
            }
        }
    }

    class ArrayWriter extends FieldWriter {
        final FieldWriter fieldWriter;

        ArrayWriter(FieldWriter fieldWriter) {
            this.fieldWriter = fieldWriter;
        }

        @Override
        final void writeRawValue(Object value) {
            throw new UnsupportedOperationException("Array has no raw value");
        }

        @Override
        final void writeField(Object value) {
            List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return;
            }

            recordConsumer.startField(fieldName, index);
            recordConsumer.startGroup();

            recordConsumer.startField("list", 0); // This is the wrapper group for the array field
            for (Object listEntry : list) {
                recordConsumer.startGroup();
                recordConsumer.startField("element", 0); // This is the mandatory inner field

                fieldWriter.writeRawValue(listEntry);

                recordConsumer.endField("element", 0);
                recordConsumer.endGroup();
            }
            recordConsumer.endField("list", 0);

            recordConsumer.endGroup();
            recordConsumer.endField(fieldName, index);
        }
    }

    /**
     * The RepeatedWriter is used to write collections (lists and maps) using the old style (without
     * LIST and MAP wrappers).
     */
    class RepeatedWriter extends FieldWriter {
        final FieldWriter fieldWriter;

        RepeatedWriter(FieldWriter fieldWriter) {
            this.fieldWriter = fieldWriter;
        }

        @Override
        final void writeRawValue(Object value) {
            throw new UnsupportedOperationException("Array has no raw value");
        }

        @Override
        final void writeField(Object value) {
            List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return;
            }

            recordConsumer.startField(fieldName, index);

            for (Object listEntry : list) {
                fieldWriter.writeRawValue(listEntry);
            }

            recordConsumer.endField(fieldName, index);
        }
    }

    /** validates mapping between protobuffer fields and parquet fields. */
    private void validatedMapping(Descriptor descriptor, GroupType parquetSchema) {
        List<FieldDescriptor> allFields = descriptor.getFields();

        for (FieldDescriptor fieldDescriptor : allFields) {
            String fieldName = fieldDescriptor.getName();
            int fieldIndex = fieldDescriptor.getIndex();
            int parquetIndex = parquetSchema.getFieldIndex(fieldName);
            if (fieldIndex != parquetIndex) {
                String message =
                        "FieldIndex mismatch name="
                                + fieldName
                                + ": "
                                + fieldIndex
                                + " != "
                                + parquetIndex;
                throw new IncompatibleSchemaModificationException(message);
            }
        }
    }

    class StringWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            Binary binaryString = Binary.fromString((String) value);
            recordConsumer.addBinary(binaryString);
        }
    }

    class IntWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            recordConsumer.addInteger((Integer) value);
        }
    }

    class LongWriter extends FieldWriter {

        @Override
        final void writeRawValue(Object value) {
            recordConsumer.addLong((Long) value);
        }
    }

    class MapWriter extends FieldWriter {

        private final FieldWriter keyWriter;
        private final FieldWriter valueWriter;

        public MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
            super();
            this.keyWriter = keyWriter;
            this.valueWriter = valueWriter;
        }

        @Override
        final void writeRawValue(Object value) {
            Collection<Message> collection = (Collection<Message>) value;
            if (collection.isEmpty()) {
                return;
            }
            recordConsumer.startField(fieldName, index);
            recordConsumer.startGroup();

            recordConsumer.startField(
                    "key_value", 0); // This is the wrapper group for the map field
            for (Message msg : collection) {
                recordConsumer.startGroup();

                final Descriptor descriptorForType = msg.getDescriptorForType();
                final FieldDescriptor keyDesc = descriptorForType.findFieldByName("key");
                final FieldDescriptor valueDesc = descriptorForType.findFieldByName("value");

                keyWriter.writeField(msg.getField(keyDesc));
                valueWriter.writeField(msg.getField(valueDesc));

                recordConsumer.endGroup();
            }

            recordConsumer.endField("key_value", 0);

            recordConsumer.endGroup();
            recordConsumer.endField(fieldName, index);
        }
    }

    class FloatWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            recordConsumer.addFloat((Float) value);
        }
    }

    class DoubleWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            recordConsumer.addDouble((Double) value);
        }
    }

    class EnumWriter extends FieldWriter {
        Map<String, Integer> enumNameNumberPairs;

        public EnumWriter(Descriptors.EnumDescriptor enumType) {
            if (protoEnumBookKeeper.containsKey(enumType.getFullName())) {
                enumNameNumberPairs = protoEnumBookKeeper.get(enumType.getFullName());
            } else {
                enumNameNumberPairs = new HashMap<>();
                protoEnumBookKeeper.put(enumType.getFullName(), enumNameNumberPairs);
            }
        }

        @Override
        final void writeRawValue(Object value) {
            Descriptors.EnumValueDescriptor enumValueDesc = (Descriptors.EnumValueDescriptor) value;
            Binary binary = Binary.fromString(enumValueDesc.getName());
            recordConsumer.addBinary(binary);
            enumNameNumberPairs.putIfAbsent(enumValueDesc.getName(), enumValueDesc.getNumber());
        }
    }

    class BooleanWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            recordConsumer.addBoolean((Boolean) value);
        }
    }

    class BinaryWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            // Non-ByteString values can happen when recursions gets truncated.
            ByteString byteString =
                    value instanceof ByteString
                            ? (ByteString) value
                            // TODO: figure out a way to use MessageOrBuilder
                            : value instanceof Message
                                    ? ((Message) value).toByteString()
                                    // Worst-case, just dump as plain java string.
                                    : ByteString.copyFromUtf8(value.toString());
            Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
            recordConsumer.addBinary(binary);
        }
    }

    class TimestampWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            Timestamp timestamp = (Timestamp) value;
            recordConsumer.addLong(Timestamps.toNanos(timestamp));
        }
    }

    class DateWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            Date date = (Date) value;
            LocalDate localDate = LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
            recordConsumer.addInteger((int) localDate.toEpochDay());
        }
    }

    class TimeWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            com.google.type.TimeOfDay timeOfDay = (com.google.type.TimeOfDay) value;
            LocalTime localTime =
                    LocalTime.of(
                            timeOfDay.getHours(),
                            timeOfDay.getMinutes(),
                            timeOfDay.getSeconds(),
                            timeOfDay.getNanos());
            recordConsumer.addLong(localTime.toNanoOfDay());
        }
    }

    class DoubleValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            recordConsumer.addDouble(((DoubleValue) value).getValue());
        }
    }

    class FloatValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            recordConsumer.addFloat(((FloatValue) value).getValue());
        }
    }

    class Int64ValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            recordConsumer.addLong(((Int64Value) value).getValue());
        }
    }

    class UInt64ValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            recordConsumer.addLong(((UInt64Value) value).getValue());
        }
    }

    class Int32ValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            recordConsumer.addInteger(((Int32Value) value).getValue());
        }
    }

    class UInt32ValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            recordConsumer.addLong(((UInt32Value) value).getValue());
        }
    }

    class BoolValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            recordConsumer.addBoolean(((BoolValue) value).getValue());
        }
    }

    class StringValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            Binary binaryString = Binary.fromString(((StringValue) value).getValue());
            recordConsumer.addBinary(binaryString);
        }
    }

    class BytesValueWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            byte[] byteArray = ((BytesValue) value).getValue().toByteArray();
            Binary binary = Binary.fromConstantByteArray(byteArray);
            recordConsumer.addBinary(binary);
        }
    }

    private FieldWriter unknownType(FieldDescriptor fieldDescriptor) {
        String exceptionMsg =
                "Unknown type with descriptor \""
                        + fieldDescriptor
                        + "\" and type \""
                        + fieldDescriptor.getJavaType()
                        + "\".";
        throw new InvalidRecordException(exceptionMsg);
    }
}

/**
 * Minimal schema converter extracting only needed behavior for the patched support. For the test
 * cases we only rely on primitive field mappings, so we can forward directly to the real converter
 * if present; else implement minimal mapping. To minimize risk, we reflectively invoke the original
 * ProtoSchemaConverter if available.
 */
class PatchedProtoSchemaConverter {
    private final ParquetConfiguration configuration;

    PatchedProtoSchemaConverter(ParquetConfiguration configuration) {
        this.configuration = configuration;
    }

    MessageType convert(Descriptor descriptor) {
        try {
            Class<?> clazz = Class.forName("org.apache.parquet.proto.ProtoSchemaConverter");
            Object inst =
                    clazz.getConstructor(ParquetConfiguration.class).newInstance(configuration);
            return (MessageType)
                    clazz.getMethod("convert", Descriptor.class).invoke(inst, descriptor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delegate to ProtoSchemaConverter", e);
        }
    }
}
