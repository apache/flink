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
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class AvroKryoSerializerTests {
    public static <T> T flinkToBytesAndBack(TypeSerializer<T> serializer, T originalObject)
            throws IOException {
        // Serialize to byte array
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(out)) {
            serializer.serialize(originalObject, outView);
        }

        try (DataInputViewStreamWrapper inView =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(out.toByteArray()))) {
            return serializer.deserialize(inView);
        }
    }

    public static Object kryoToBytesAndBack(Kryo kryo, Object originalObject) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (Output output = new Output(byteArrayOutputStream)) {
            kryo.writeClassAndObject(output, originalObject);
            output.flush();
        }

        try (Input input =
                new Input(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))) {
            return kryo.readClassAndObject(input);
        }
    }

    public <T> void testTypeSerialization(Class<T> javaClass, T originalObject) throws IOException {
        ExecutionConfig ec = new ExecutionConfig();
        SerializerConfig serializerConfig = ec.getSerializerConfig();

        TypeInformation<T> typeInformation = TypeExtractor.createTypeInfo(javaClass);
        Serializers.recursivelyRegisterType(
                typeInformation.getTypeClass(), serializerConfig, new HashSet<>());

        TypeSerializer<T> serializer = typeInformation.createSerializer(serializerConfig);

        T deserializedObject = flinkToBytesAndBack(serializer, originalObject);
        Assertions.assertEquals(originalObject, deserializedObject);

        T copiedObject = serializer.copy(originalObject);
        Assertions.assertEquals(originalObject, copiedObject);

        if (serializer instanceof KryoSerializer) {
            KryoSerializer<T> kryoSerializer = (KryoSerializer<T>) serializer;

            T kryoDeserializedObject =
                    javaClass.cast(kryoToBytesAndBack(kryoSerializer.getKryo(), originalObject));
            Assertions.assertEquals(originalObject, kryoDeserializedObject);
        }
    }

    @Test
    public void testGenericRecord() throws IOException {
        final Schema timestampMilliType =
                LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

        final Schema recordSchema =
                SchemaBuilder.record("demoRecord")
                        .namespace("demo")
                        .fields()
                        .name("timestamp")
                        .type(timestampMilliType)
                        .noDefault()
                        .name("arrayOfStrings")
                        .type()
                        .array()
                        .items()
                        .stringType()
                        .noDefault()
                        .endRecord();

        final GenericRecord testRecordA =
                new GenericRecordBuilder(recordSchema)
                        .set(
                                "timestamp",
                                LocalDateTime.of(2025, 2, 20, 16, 0, 0)
                                        .toInstant(ZoneOffset.UTC)
                                        .toEpochMilli())
                        .set("arrayOfStrings", Arrays.asList("aaa", "bbb", "ccc"))
                        .build();

        testTypeSerialization(GenericRecord.class, testRecordA);
    }

    @Test
    public void testHomogeneousRecordArray() throws IOException {
        final Schema timestampMilliType =
                LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

        final Schema recordSchema =
                SchemaBuilder.record("demoRecord")
                        .namespace("demo")
                        .fields()
                        .name("timestamp")
                        .type(timestampMilliType)
                        .noDefault()
                        .name("arrayOfStrings")
                        .type()
                        .array()
                        .items()
                        .stringType()
                        .noDefault()
                        .endRecord();

        final GenericRecord testRecordA =
                new GenericRecordBuilder(recordSchema)
                        .set(
                                "timestamp",
                                LocalDateTime.of(2025, 2, 20, 16, 0, 0)
                                        .toInstant(ZoneOffset.UTC)
                                        .toEpochMilli())
                        .set("arrayOfStrings", Arrays.asList("aaa", "bbb", "ccc"))
                        .build();

        final GenericRecord testRecordB =
                new GenericRecordBuilder(recordSchema)
                        .set(
                                "timestamp",
                                LocalDateTime.of(2025, 1, 20, 14, 0, 0)
                                        .toInstant(ZoneOffset.UTC)
                                        .toEpochMilli())
                        .set("arrayOfStrings", Arrays.asList("zzz", "yyy", "xxx"))
                        .build();

        Schema arraySchema = Schema.createArray(recordSchema);
        var genericRecordArray =
                new GenericData.Array<>(arraySchema, List.of(testRecordA, testRecordB));
        testTypeSerialization(GenericData.Array.class, genericRecordArray);
    }

    @Test
    public void testHeterogeneousArray() throws IOException {
        Schema unionSchema = SchemaBuilder.unionOf().intType().and().stringType().endUnion();
        Schema arraySchema = Schema.createArray(unionSchema);

        var heterogeneousArray =
                new GenericData.Array<Object>(
                        arraySchema, List.of(new Utf8("aaa"), 123, new Utf8("zzz"), 456));
        testTypeSerialization(GenericData.Array.class, heterogeneousArray);
    }

    @Test
    public void testNullValues() throws IOException {
        testTypeSerialization(Schema.class, null);
        testTypeSerialization(GenericRecord.class, null);
        testTypeSerialization(GenericData.Array.class, null);
    }
}
