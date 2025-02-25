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
import org.apache.avro.generic.GenericData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;

/** Utilities for integrating Avro serializers in Kryo. */
public class AvroKryoSerializerUtils extends AvroUtils {

    @Override
    public void addAvroSerializersIfRequired(SerializerConfig reg, Class<?> type) {
        if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(type)
                || org.apache.avro.generic.GenericData.Record.class.isAssignableFrom(type)) {

            // Avro POJOs contain java.util.List which have GenericData.Array as their runtime type
            // because Kryo is not able to serialize them properly, we use this serializer for them
            ((SerializerConfigImpl) reg)
                    .registerTypeWithKryoSerializer(
                            GenericData.Array.class, AvroGenericDataArraySerializer.class);

            // We register this serializer for users who want to use untyped Avro records
            // (GenericData.Record).
            // Kryo is able to serialize everything in there, except for the Schema.
            // This serializer is very slow, but using the GenericData.Records of Kryo is in general
            // a bad idea.
            // we add the serializer as a default serializer because Avro is using a private
            // sub-type at runtime.
            ((SerializerConfigImpl) reg)
                    .addDefaultKryoSerializer(Schema.class, AvroSchemaSerializer.class);
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

    /**
     * Slow serialization approach for Avro schemas. This is only used with {{@link
     * org.apache.avro.generic.GenericData.Record}} types. Having this serializer, we are able to
     * handle avro Records.
     */
    public static class AvroSchemaSerializer extends Serializer<Schema> implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(Kryo kryo, Output output, Schema object) {
            String schemaAsString = object.toString(false);
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

    public static class AvroGenericDataArraySerializer
            extends CollectionSerializer<GenericData.Array> implements Serializable {
        @Override
        public void write(Kryo kryo, Output output, GenericData.Array collection) {
            String schemaAsString = collection.getSchema().toString(false);
            output.writeString(schemaAsString);

            int length = collection.size();
            output.writeVarInt(length + 1, true);

            // Efficiency could be improved for cases where all elements are the same type.
            for (Object element : collection) {
                kryo.writeClassAndObject(output, element);
            }
        }

        @Override
        public GenericData.Array read(
                Kryo kryo, Input input, Class<? extends GenericData.Array> type) {
            String schemaAsString = input.readString();
            Schema.Parser schemaParser = new Schema.Parser();
            Schema schema = schemaParser.parse(schemaAsString);

            int lengthPlusOne = input.readVarInt(true);
            int length = lengthPlusOne - 1;

            // Efficiency could be improved for cases where all elements are the same type.
            ArrayList<Object> collection = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                collection.add(kryo.readClassAndObject(input));
            }
            return new GenericData.Array(schema, collection);
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
