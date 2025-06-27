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

package org.apache.flink.formats.avro.utils;

import org.apache.flink.api.common.SerializableSerializer;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.AvroUtils;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;

/** Utilities for integrating Avro serializers in Kryo. */
public class AvroKryoSerializerUtils extends AvroUtils {

    @Override
    public void addAvroSerializersIfRequired(SerializerConfig reg, Class<?> type) {
        if (Schema.class.isAssignableFrom(type)
                || GenericRecord.class.isAssignableFrom(type)
                || GenericArray.class.isAssignableFrom(type)) {
            SerializerConfigImpl regImpl = ((SerializerConfigImpl) reg);

            // We register custom Kryo serializers for users who want to use Avro generic types.
            regImpl.registerTypeWithKryoSerializer(
                    GenericData.Record.class, AvroGenericRecordSerializer.class);
            regImpl.registerTypeWithKryoSerializer(
                    GenericData.Array.class, AvroGenericDataArraySerializer.class);
            regImpl.addDefaultKryoSerializer(Schema.class, AvroSchemaSerializer.class);
        }
    }

    @Override
    public void addAvroGenericDataArrayRegistration(
            LinkedHashMap<String, KryoRegistration> kryoRegistrations) {
        kryoRegistrations.put(
                GenericData.Array.class.getName(),
                new KryoRegistration(
                        GenericData.Array.class,
                        new SerializableSerializer<>(new AvroGenericDataArraySerializer())));
    }

    @Override
    public <T> TypeSerializer<T> createAvroSerializer(Class<T> type) {
        return new AvroSerializer<>(type);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> TypeInformation<T> createAvroTypeInfo(Class<T> type) {
        // we have to be raw here because we cannot have "<T extends SpecificRecordBase>" in
        // the interface of AvroUtils
        return new AvroTypeInfo(type);
    }

    static <T> byte[] avroSerializeToBytes(Schema schema, T o) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<T> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(o, encoder);
        encoder.flush();

        return out.toByteArray();
    }

    static <T> T avroDeserializeFromBytes(Schema schema, byte[] bytes) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            DatumReader<T> datumReader = new GenericDatumReader<>(schema);
            return datumReader.read(null, decoder);
        }
    }

    /**
     * Slow serialization approach for Avro schemas. This is only used with {{@link
     * org.apache.avro.generic.GenericData.Record}} types. Having this serializer, we are able to
     * handle avro Records.
     */
    public static class AvroSchemaSerializer extends Serializer<Schema> implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(Kryo kryo, Output output, Schema object) {
            String schemaAsString = object.toString();
            output.writeString(schemaAsString);
        }

        @Override
        public Schema read(Kryo kryo, Input input, Class<? extends Schema> type) {
            String schemaAsString = input.readString();
            // the parser seems to be stateful, to we need a new one for every type.
            Schema.Parser sParser = new Schema.Parser();
            return sParser.parse(schemaAsString);
        }
    }

    public static class AvroGenericRecordSerializer extends Serializer<GenericData.Record>
            implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(Kryo kryo, Output output, GenericData.Record record) {
            String schemaAsString = record.getSchema().toString();
            output.writeString(schemaAsString);

            try {
                byte[] bytes = avroSerializeToBytes(record.getSchema(), record);
                output.writeVarInt(bytes.length, true);
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public GenericData.Record read(
                Kryo kryo, Input input, Class<? extends GenericData.Record> type) {
            String schemaAsString = input.readString();
            Schema.Parser schemaParser = new Schema.Parser();
            Schema schema = schemaParser.parse(schemaAsString);

            int bytesLength = input.readVarInt(true);
            byte[] bytes = input.readBytes(bytesLength);
            try {
                return avroDeserializeFromBytes(schema, bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class AvroGenericDataArraySerializer
            extends CollectionSerializer<GenericData.Array> implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(Kryo kryo, Output output, GenericData.Array array) {
            String schemaAsString = array.getSchema().toString();
            output.writeString(schemaAsString);

            try {
                byte[] bytes = avroSerializeToBytes(array.getSchema(), array);
                output.writeVarInt(bytes.length, true);
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public GenericData.Array read(
                Kryo kryo, Input input, Class<? extends GenericData.Array> type) {
            String schemaAsString = input.readString();
            Schema.Parser schemaParser = new Schema.Parser();
            Schema schema = schemaParser.parse(schemaAsString);

            int bytesLength = input.readVarInt(true);
            byte[] bytes = input.readBytes(bytesLength);
            try {
                return avroDeserializeFromBytes(schema, bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public GenericData.Array createCopy(Kryo kryo, GenericData.Array original) {
            String schemaAsString = original.getSchema().toString(false);
            Schema.Parser schemaParser = new Schema.Parser();
            Schema schema = schemaParser.parse(schemaAsString);

            return new GenericData.Array(schema, Collections.emptyList());
        }
    }
}
