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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.AvroUtils;
import org.apache.flink.api.java.typeutils.runtime.Kryo5Registration;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.Serializable;
import java.util.LinkedHashMap;

/** Utilities for integrating Avro serializers in Kryo. */
public class AvroKryoSerializerUtils extends AvroUtils {

    @Override
    public void addAvroSerializersIfRequired(ExecutionConfig reg, Class<?> type) {
        if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(type)
                || org.apache.avro.generic.GenericData.Record.class.isAssignableFrom(type)) {

            // Avro POJOs contain java.util.List which have GenericData.Array as their runtime type
            // because Kryo is not able to serialize them properly, we use this serializer for them
            reg.registerTypeWithKryoSerializer(
                    GenericData.Array.class,
                    org.apache.flink.api.java.typeutils.runtime.kryo.Serializers
                            .SpecificInstanceCollectionSerializerForArrayList.class);
            reg.registerTypeWithKryo5Serializer(
                    GenericData.Array.class,
                    org.apache.flink.api.java.typeutils.runtime.kryo5.Serializers
                            .SpecificInstanceCollectionSerializerForArrayList.class);

            // We register this serializer for users who want to use untyped Avro records
            // (GenericData.Record).
            // Kryo is able to serialize everything in there, except for the Schema.
            // This serializer is very slow, but using the GenericData.Records of Kryo is in general
            // a bad idea.
            // we add the serializer as a default serializer because Avro is using a private
            // sub-type at runtime.
            reg.addDefaultKryoSerializer(Schema.class, AvroSchemaSerializer.class);
            reg.addDefaultKryo5Serializer(Schema.class, AvroKryo5SchemaSerializer.class);
        }
    }

    @Override
    public void addAvroGenericDataArrayRegistration(
            LinkedHashMap<String, KryoRegistration> kryoRegistrations) {
        kryoRegistrations.put(
                GenericData.Array.class.getName(),
                new KryoRegistration(
                        GenericData.Array.class,
                        new ExecutionConfig.SerializableSerializer<>(
                                new org.apache.flink.api.java.typeutils.runtime.kryo.Serializers
                                        .SpecificInstanceCollectionSerializerForArrayList())));
    }

    @Override
    public void addAvroGenericDataArrayRegistration5(
            LinkedHashMap<String, Kryo5Registration> kryoRegistrations) {
        kryoRegistrations.put(
                GenericData.Array.class.getName(),
                new Kryo5Registration(
                        GenericData.Array.class,
                        new ExecutionConfig.SerializableKryo5Serializer<>(
                                new org.apache.flink.api.java.typeutils.runtime.kryo5.Serializers
                                        .SpecificInstanceCollectionSerializerForArrayList())));
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
    public static class AvroSchemaSerializer extends com.esotericsoftware.kryo.Serializer<Schema>
            implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(
                com.esotericsoftware.kryo.Kryo kryo,
                com.esotericsoftware.kryo.io.Output output,
                Schema object) {
            String schemaAsString = object.toString(false);
            output.writeString(schemaAsString);
        }

        @Override
        public Schema read(
                com.esotericsoftware.kryo.Kryo kryo,
                com.esotericsoftware.kryo.io.Input input,
                Class<Schema> type) {
            String schemaAsString = input.readString();
            // the parser seems to be stateful, to we need a new one for every type.
            Schema.Parser sParser = new Schema.Parser();
            return sParser.parse(schemaAsString);
        }
    }

    /**
     * Slow serialization approach for Avro schemas. This is only used with {{@link
     * org.apache.avro.generic.GenericData.Record}} types. Having this serializer, we are able to
     * handle avro Records.
     */
    public static class AvroKryo5SchemaSerializer
            extends com.esotericsoftware.kryo.kryo5.Serializer<Schema> implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(
                com.esotericsoftware.kryo.kryo5.Kryo kryo,
                com.esotericsoftware.kryo.kryo5.io.Output output,
                Schema object) {
            String schemaAsString = object.toString(false);
            output.writeString(schemaAsString);
        }

        @Override
        public Schema read(
                com.esotericsoftware.kryo.kryo5.Kryo kryo,
                com.esotericsoftware.kryo.kryo5.io.Input input,
                Class<? extends Schema> type) {
            String schemaAsString = input.readString();
            // the parser seems to be stateful, to we need a new one for every type.
            Schema.Parser sParser = new Schema.Parser();
            return sParser.parse(schemaAsString);
        }
    }
}
