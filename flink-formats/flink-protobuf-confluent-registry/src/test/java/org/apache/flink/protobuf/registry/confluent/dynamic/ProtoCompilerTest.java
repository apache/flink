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

package org.apache.flink.protobuf.registry.confluent.dynamic;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import org.apache.flink.formats.protobuf.PbConstant;

import org.apache.flink.formats.protobuf.proto.ConfluentTagsProto3OuterClass;
import org.apache.flink.formats.protobuf.proto.EnumProto3OuterClass;
import org.apache.flink.formats.protobuf.proto.FlatProto2OuterClass;
import org.apache.flink.formats.protobuf.proto.FlatProto3OuterClass;
import org.apache.flink.formats.protobuf.proto.MapProto3;
import org.apache.flink.formats.protobuf.proto.MultipleFilesProto3;
import org.apache.flink.formats.protobuf.proto.MyFancyOuterClassName;
import org.apache.flink.formats.protobuf.proto.NestedProto3OuterClass;
import org.apache.flink.formats.protobuf.proto.OneofProto3;
import org.apache.flink.formats.protobuf.proto.TimestampProto3OuterClass;
import org.apache.flink.formatzz.proto.JavaPackageProto3OuterClass;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class ProtoCompilerTest {

    static public final String TEST_STRING = "test";
    static public final int TEST_INT = 42;
    static public final long TEST_LONG = 99L;
    static public final float TEST_FLOAT = 3.14f;
    static public final double TEST_DOUBLE = 2.71828;
    static public final boolean TEST_BOOL = true;
    static public final ByteString TEST_BYTES = ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03, 0x04});
    static private final Map<String, String> TEST_STRING_MAP = new HashMap<>();
    static private final String DEFAULT_PACKAGE = "org.apache.flink.formats.protobuf.proto";
    static private final int DEFAULT_SCHEMA_ID = 1;
    static private final String DEFAULT_CLASS_SUFFIX = "123";

    @BeforeEach
    public void setup() {
        TEST_STRING_MAP.put("key1", "value1");
        TEST_STRING_MAP.put("key2", "value2");
    }

    @Test
    public void flatProto3() throws Exception {
        FlatProto3OuterClass.FlatProto3 in = FlatProto3OuterClass.FlatProto3.newBuilder()
                .setString(TEST_STRING)
                .setInt(TEST_INT)
                .setLong(TEST_LONG)
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .addInts(TEST_INT)
                .setBytes(TEST_BYTES)
                .setBool(TEST_BOOL)
                .build();
        Class generatedClass = runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void flatProto3WithFieldsOmitted() throws Exception {
        FlatProto3OuterClass.FlatProto3 in = FlatProto3OuterClass.FlatProto3.newBuilder()
                .addInts(TEST_INT)
                .setBytes(TEST_BYTES)
                .setBool(TEST_BOOL)
                .build();
        runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void mapProto3() throws Exception {
        MapProto3.Proto3Map in = MapProto3.Proto3Map.newBuilder()
                .putAllMap(TEST_STRING_MAP)
                .build();
        runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void nestedProto3() throws Exception {
        NestedProto3OuterClass.NestedProto3.Nested nested = NestedProto3OuterClass.NestedProto3.Nested.newBuilder()
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .build();

        NestedProto3OuterClass.NestedProto3 in = NestedProto3OuterClass.NestedProto3.newBuilder()
                .setNested(nested)
                .setString(TEST_STRING)
                .setInt(TEST_INT)
                .setLong(TEST_LONG)
                .build();
        runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void oneOfProto3() throws Exception {
        OneofProto3.OneOfProto3 in = OneofProto3.OneOfProto3.newBuilder()
                .setA(TEST_INT)
                .setB(TEST_INT)
                .build();
        runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void enumProto3() throws Exception {
        EnumProto3OuterClass.EnumProto3 in = EnumProto3OuterClass.EnumProto3.newBuilder()
                .setInt(TEST_INT)
                .setCorpus(EnumProto3OuterClass.EnumProto3.Corpus.UNIVERSAL)
                .build();
        runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void timestampProto3() throws Exception {
        TimestampProto3OuterClass.TimestampProto3 in = TimestampProto3OuterClass.TimestampProto3.newBuilder()
                .setTs(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
                .build();
        runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void javaPackageProto3() throws Exception {
        JavaPackageProto3OuterClass.JavaPackageProto3 in = JavaPackageProto3OuterClass.JavaPackageProto3.newBuilder()
                .setString(TEST_STRING)
                .setInt(TEST_INT)
                .setLong(TEST_LONG)
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .addInts(TEST_INT)
                .setBytes(TEST_BYTES)
                .setBool(TEST_BOOL)
                .build();
        Class generatedClass = runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void multipleFilesProto3() throws Exception {
        MultipleFilesProto3.Nested nested = MultipleFilesProto3.Nested.newBuilder()
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .build();

        MultipleFilesProto3 in = MultipleFilesProto3.newBuilder()
                .setInt(TEST_INT)
                .setNested(nested)
                .build();
        Class generatedClass = runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
        Assertions.assertEquals("org.apache.flink.formats.protobuf.proto.MultipleFilesProto3_1_123$MultipleFilesProto3", generatedClass.getName());
    }

    @Test
    public void outerClassNameOptionProto3() throws Exception {
        MyFancyOuterClassName.OuterClassOptionProto3 in = MyFancyOuterClassName.OuterClassOptionProto3.newBuilder()
                .setInt(TEST_INT)
                .build();
        Class generatedClass = runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void confluentTagsProto3() throws Exception {
        ConfluentTagsProto3OuterClass.ConfluentTagsProto3 in = ConfluentTagsProto3OuterClass.ConfluentTagsProto3.newBuilder()
                .setInt(TEST_INT)
                .build();
        Class generatedClass = runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void flatProto2() throws Exception {
        FlatProto2OuterClass.FlatProto2 in = FlatProto2OuterClass.FlatProto2.newBuilder()
                .setString(TEST_STRING)
                .setInt(TEST_INT)
                .setLong(TEST_LONG)
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .addInts(TEST_INT)
                .setBytes(TEST_BYTES)
                .setBool(TEST_BOOL)
                .build();
        runCompilerTest(in, null, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void flatProto2WithFieldsOmitted() throws Exception {
        FlatProto2OuterClass.FlatProto2 in = FlatProto2OuterClass.FlatProto2.newBuilder()
                .setLong(TEST_LONG)
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .addInts(TEST_INT)
                .setBool(TEST_BOOL)
                .build();
        runCompilerTest(in, "/proto/flat_proto2.proto", DEFAULT_SCHEMA_ID);
    }

    @Test
    public void sameSchemaDifferentIds() throws Exception {
        FlatProto3OuterClass.FlatProto3 in = FlatProto3OuterClass.FlatProto3.newBuilder()
                .setString(TEST_STRING)
                .setInt(TEST_INT)
                .setLong(TEST_LONG)
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .addInts(TEST_INT)
                .setBytes(TEST_BYTES)
                .setBool(TEST_BOOL)
                .build();
        byte[] inBytes = in.toByteArray();

        ProtobufSchema schema = createProtobufSchema(in, null);
        ProtoCompiler protoCompiler = new ProtoCompiler(DEFAULT_CLASS_SUFFIX);
        Class generatedClass1 = protoCompiler.generateMessageClass(schema, 1);
        Class generatedClass2 = protoCompiler.generateMessageClass(schema, 2);

        byte[] reprocessedBytes1 = processOriginalBytesWithGeneratedClass(generatedClass1, inBytes);
        byte[] reprocessedBytes2 = processOriginalBytesWithGeneratedClass(generatedClass2, inBytes);

        Assertions.assertArrayEquals(
                inBytes,
                reprocessedBytes1,
                "Input and output1 messages are not the same");
        Assertions.assertArrayEquals(
                inBytes,
                reprocessedBytes2,
                "Input and output2 messages are not the same");

        assertGeneratedClassName(in, generatedClass1, 1);
        assertGeneratedClassName(in, generatedClass2, 2);
    }

    @Test
    public void noClassNameClashForSameSchemaOnDifferentThreads() throws Exception {
        FlatProto3OuterClass.FlatProto3 in = FlatProto3OuterClass.FlatProto3.newBuilder()
                .setString(TEST_STRING)
                .setInt(TEST_INT)
                .setLong(TEST_LONG)
                .setFloat(TEST_FLOAT)
                .setDouble(TEST_DOUBLE)
                .addInts(TEST_INT)
                .setBytes(TEST_BYTES)
                .setBool(TEST_BOOL)
                .build();
        byte[] inBytes = in.toByteArray();

        Queue<Class> classes = new ConcurrentLinkedQueue<>();

        Thread thread1 = new Thread(() -> {
            try {
                ProtobufSchema schema = createProtobufSchema(in, null);
                ProtoCompiler protoCompiler = new ProtoCompiler();
                Class generatedClass = protoCompiler.generateMessageClass(schema, DEFAULT_SCHEMA_ID);
                classes.add(generatedClass);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                ProtobufSchema schema = createProtobufSchema(in, null);
                ProtoCompiler protoCompiler = new ProtoCompiler();
                Class generatedClass = protoCompiler.generateMessageClass(schema, DEFAULT_SCHEMA_ID);
                classes.add(generatedClass);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Class generatedClass1 = classes.poll();
        Class generatedClass2 = classes.poll();

        byte[] reprocessedBytes1 = processOriginalBytesWithGeneratedClass(generatedClass1, inBytes);
        byte[] reprocessedBytes2 = processOriginalBytesWithGeneratedClass(generatedClass2, inBytes);

        Assertions.assertArrayEquals(
                inBytes,
                reprocessedBytes1,
                "Input and output1 messages are not the same");
        Assertions.assertArrayEquals(
                inBytes,
                reprocessedBytes2,
                "Input and output2 messages are not the same");

        Assertions.assertNotEquals(generatedClass1.getName(), generatedClass2.getName());

    }

    private Class runCompilerTest(Message message, String protoFileName, int fakeSchemaId) throws Exception {
        byte[] inBytes = message.toByteArray();

        // Fixtures
        ProtobufSchema schema = createProtobufSchema(message, protoFileName);
        ProtoCompiler protoCompiler = new ProtoCompiler(DEFAULT_CLASS_SUFFIX);

        // Call the target method
        Class messageClass = protoCompiler.generateMessageClass(schema, fakeSchemaId);

        // Parse the original message and then serialize it back
        byte[] outBytes = processOriginalBytesWithGeneratedClass(messageClass, inBytes);

        // Check if the original and the rebuilt messages are the same
        Assertions.assertArrayEquals(
                inBytes,
                outBytes,
                "Input and output messages are not the same");
        return messageClass;
    }

    private byte[] processOriginalBytesWithGeneratedClass(Class generatedClass, byte[] inBytes) throws Exception {
        Method parseFromMethod = generatedClass.getMethod(PbConstant.PB_METHOD_PARSE_FROM, byte[].class);
        Method toByteArrayMethod = generatedClass.getMethod("toByteArray");

        // Parse the original message and then serialize it back
        return (byte[]) toByteArrayMethod.invoke(parseFromMethod.invoke(null, inBytes));
    }

    public void assertGeneratedClassName(Message originalMessage, Class generatedClass, int fakeSchemaId) {
        String originalMessageClassName = originalMessage.getClass().getName();
        String[] originalMessageClassNameParts = originalMessageClassName.split("\\$");
        String originalMessageInnerClassName = originalMessageClassNameParts[originalMessageClassNameParts.length - 1];
        String generatedClassName = generatedClass.getName();
        String expectedGeneratedClassName = String.format(
                "%s.%s_%d_%s$%s",
                DEFAULT_PACKAGE, originalMessageInnerClassName, fakeSchemaId, DEFAULT_CLASS_SUFFIX, originalMessageInnerClassName
        );
        Assertions.assertEquals(expectedGeneratedClassName, generatedClassName);
    }

    private ProtobufSchema createProtobufSchema(Message message, String protoFileName) {
        // This method didn't work for proto2 generated messages, so we will construct the
        // ProtobufSchema directly from the proto file contents.
        if (protoFileName == null) {
            return new ProtobufSchema(message.getDescriptorForType());
        }

        InputStream protoFileStream = this.getClass().getResourceAsStream(protoFileName);
        if (protoFileStream == null) {
            throw new IllegalArgumentException("Could not find the proto file: " + protoFileName);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(protoFileStream, StandardCharsets.UTF_8));
        String protoFileContents = reader.lines().collect(Collectors.joining("\n"));
        return new ProtobufSchema(protoFileContents);
    }
}
