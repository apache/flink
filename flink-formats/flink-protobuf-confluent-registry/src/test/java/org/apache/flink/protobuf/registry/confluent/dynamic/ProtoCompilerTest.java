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

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.protobuf.registry.confluent.TestUtils.DEFAULT_CLASS_SUFFIX;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.DEFAULT_PACKAGE;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.DEFAULT_SCHEMA_ID;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_BOOL;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_BYTES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_DOUBLE;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_FLOAT;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_INT;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_LONG;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_STRING;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.USE_DEFAULT_PROTO_INCLUDES;

public class ProtoCompilerTest {

    private static final Map<String, String> TEST_STRING_MAP = new HashMap<>();

    @BeforeEach
    public void setup() {
        TEST_STRING_MAP.put("key1", "value1");
        TEST_STRING_MAP.put("key2", "value2");
    }

    @Test
    public void flatProto3() throws Exception {
        FlatProto3OuterClass.FlatProto3 in =
                FlatProto3OuterClass.FlatProto3.newBuilder()
                        .setString(TEST_STRING)
                        .setInt(TEST_INT)
                        .setLong(TEST_LONG)
                        .setFloat(TEST_FLOAT)
                        .setDouble(TEST_DOUBLE)
                        .addInts(TEST_INT)
                        .setBytes(TEST_BYTES)
                        .setBool(TEST_BOOL)
                        .build();
        Class generatedClass = runCompilerTest(in, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void flatProto3WithFieldsOmitted() throws Exception {
        FlatProto3OuterClass.FlatProto3 in =
                FlatProto3OuterClass.FlatProto3.newBuilder()
                        .addInts(TEST_INT)
                        .setBytes(TEST_BYTES)
                        .setBool(TEST_BOOL)
                        .build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void mapProto3() throws Exception {
        MapProto3.Proto3Map in =
                MapProto3.Proto3Map.newBuilder().putAllMap(TEST_STRING_MAP).build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void nestedProto3() throws Exception {
        NestedProto3OuterClass.NestedProto3.Nested nested =
                NestedProto3OuterClass.NestedProto3.Nested.newBuilder()
                        .setFloat(TEST_FLOAT)
                        .setDouble(TEST_DOUBLE)
                        .build();

        NestedProto3OuterClass.NestedProto3 in =
                NestedProto3OuterClass.NestedProto3.newBuilder()
                        .setNested(nested)
                        .setString(TEST_STRING)
                        .setInt(TEST_INT)
                        .setLong(TEST_LONG)
                        .build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void oneOfProto3() throws Exception {
        OneofProto3.OneOfProto3 in =
                OneofProto3.OneOfProto3.newBuilder().setA(TEST_INT).setB(TEST_INT).build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void enumProto3() throws Exception {
        EnumProto3OuterClass.EnumProto3 in =
                EnumProto3OuterClass.EnumProto3.newBuilder()
                        .setInt(TEST_INT)
                        .setCorpus(EnumProto3OuterClass.EnumProto3.Corpus.UNIVERSAL)
                        .build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void timestampProto3() throws Exception {
        TimestampProto3OuterClass.TimestampProto3 in =
                TimestampProto3OuterClass.TimestampProto3.newBuilder()
                        .setTs(
                                Timestamp.newBuilder()
                                        .setSeconds(System.currentTimeMillis() / 1000)
                                        .build())
                        .build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void javaPackageProto3() throws Exception {
        JavaPackageProto3OuterClass.JavaPackageProto3 in =
                JavaPackageProto3OuterClass.JavaPackageProto3.newBuilder()
                        .setString(TEST_STRING)
                        .setInt(TEST_INT)
                        .setLong(TEST_LONG)
                        .setFloat(TEST_FLOAT)
                        .setDouble(TEST_DOUBLE)
                        .addInts(TEST_INT)
                        .setBytes(TEST_BYTES)
                        .setBool(TEST_BOOL)
                        .build();
        Class generatedClass = runCompilerTest(in, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void multipleFilesProto3() throws Exception {
        MultipleFilesProto3.Nested nested =
                MultipleFilesProto3.Nested.newBuilder()
                        .setFloat(TEST_FLOAT)
                        .setDouble(TEST_DOUBLE)
                        .build();

        MultipleFilesProto3 in =
                MultipleFilesProto3.newBuilder().setInt(TEST_INT).setNested(nested).build();
        Class generatedClass = runCompilerTest(in, DEFAULT_SCHEMA_ID);
        Assertions.assertEquals(
                "org.apache.flink.formats.protobuf.proto.MultipleFilesProto3_1_123$MultipleFilesProto3",
                generatedClass.getName());
    }

    @Test
    public void outerClassNameOptionProto3() throws Exception {
        MyFancyOuterClassName.OuterClassOptionProto3 in =
                MyFancyOuterClassName.OuterClassOptionProto3.newBuilder().setInt(TEST_INT).build();
        Class generatedClass = runCompilerTest(in, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void confluentTagsProto3() throws Exception {
        ConfluentTagsProto3OuterClass.ConfluentTagsProto3 in =
                ConfluentTagsProto3OuterClass.ConfluentTagsProto3.newBuilder()
                        .setInt(TEST_INT)
                        .build();
        Class generatedClass = runCompilerTest(in, DEFAULT_SCHEMA_ID);
        assertGeneratedClassName(in, generatedClass, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void confluentTagsWithMissingImportProto3() throws Exception {
        // the schemas registered by Debezium don't always seem to have the import statement in
        // the definition
        String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package org.apache.flink.formats.protobuf.proto;\n"
                        + "\n"
                        + "message ConfluentTagsWithMissingImportProto3 {\n"
                        + "  int32 int = 1 [(confluent.field_meta) = {\n"
                        + "    params: [\n"
                        + "      {\n"
                        + "        key: \"connect.type\",\n"
                        + "        value: \"int16\"\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }];\n"
                        + "}";

        ProtobufSchema schema = new ProtobufSchema(schemaStr);
        ProtoCompiler protoCompiler =
                new ProtoCompiler(DEFAULT_CLASS_SUFFIX, USE_DEFAULT_PROTO_INCLUDES);

        // We just want to check that the compiler doesn't throw an exception
        protoCompiler.generateMessageClass(schema, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void confluentTagsWithImportProto3() throws Exception {
        // the schemas registered by Debezium don't always seem to have the import statement in
        // the definition
        String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package org.apache.flink.formats.protobuf.proto;\n"
                        + "import \"confluent/meta.proto\";\n"
                        + "\n"
                        + "message ConfluentTagsWithImportProto3 {\n"
                        + "  int32 int = 1 [(confluent.field_meta) = {\n"
                        + "    params: [\n"
                        + "      {\n"
                        + "        key: \"connect.type\",\n"
                        + "        value: \"int16\"\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }];\n"
                        + "}";

        ProtobufSchema schema = new ProtobufSchema(schemaStr);
        ProtoCompiler protoCompiler =
                new ProtoCompiler(DEFAULT_CLASS_SUFFIX, USE_DEFAULT_PROTO_INCLUDES);

        // We just want to check that the compiler doesn't throw an exception
        protoCompiler.generateMessageClass(schema, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void confluentTagsWithoutDefaultIncludesProto3() throws Exception {
        String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package org.apache.flink.formats.protobuf.proto;\n"
                        + "import \"confluent/meta.proto\";\n"
                        + "\n"
                        + "message ConfluentTagsWithImportProto3 {\n"
                        + "  int32 int = 1 [(confluent.field_meta) = {\n"
                        + "    params: [\n"
                        + "      {\n"
                        + "        key: \"connect.type\",\n"
                        + "        value: \"int16\"\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }];\n"
                        + "}";

        ProtobufSchema schema = new ProtobufSchema(schemaStr);
        boolean useDefaultProtoIncludes = false;
        ProtoCompiler protoCompiler =
                new ProtoCompiler(DEFAULT_CLASS_SUFFIX, useDefaultProtoIncludes);

        Assertions.assertThrows(
                RuntimeException.class,
                () -> protoCompiler.generateMessageClass(schema, DEFAULT_SCHEMA_ID));
    }

    @Test
    public void customIncludesProto3() throws Exception {
        String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package org.apache.flink.formats.protobuf.proto;\n"
                        + "import \"confluent/meta.proto\";\n"
                        + "\n"
                        + "message ConfluentTagsWithImportProto3 {\n"
                        + "  int32 int = 1 [(confluent.field_meta) = {\n"
                        + "    params: [\n"
                        + "      {\n"
                        + "        key: \"connect.type\",\n"
                        + "        value: \"int16\"\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }];\n"
                        + "}";

        ProtobufSchema schema = new ProtobufSchema(schemaStr);
        boolean useDefaultProtoIncludes = false;
        String[] customProtoIncludes =
                new String[] {
                    "/confluent/meta.proto",
                    "/confluent/type/decimal.proto",
                    "/google/protobuf/descriptor.proto"
                };
        ProtoCompiler protoCompiler =
                new ProtoCompiler(
                        DEFAULT_CLASS_SUFFIX, useDefaultProtoIncludes, customProtoIncludes);

        // We just want to check that the compiler doesn't throw an exception
        protoCompiler.generateMessageClass(schema, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void optionalWithConfluentTagsProto3() throws Exception {
        String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package org.apache.flink.formats.protobuf.proto;\n"
                        + "\n"
                        + "message Envelope {\n"
                        + "  optional Value before = 1;\n"
                        + "\n"
                        + "  message Value {\n"
                        + "    int64 id = 1 [deprecated = false];\n"
                        + "    optional int64 other_id = 2 ;\n"
                        + "    optional int32 is_published = 13 [\n"
                        + "      deprecated = false,\n"
                        + "      (confluent.field_meta) = {\n"
                        + "        params: [\n"
                        + "          {\n"
                        + "            key: \"connect.type\",\n"
                        + "            value: \"int16\"\n"
                        + "          }\n"
                        + "        ]\n"
                        + "      }\n"
                        + "    ];"
                        + "  }\n"
                        + "}";

        ProtobufSchema schema = new ProtobufSchema(schemaStr);
        ProtoCompiler protoCompiler =
                new ProtoCompiler(DEFAULT_CLASS_SUFFIX, USE_DEFAULT_PROTO_INCLUDES);

        // We just want to check that the compiler doesn't throw an exception
        protoCompiler.generateMessageClass(schema, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void flatProto2() throws Exception {
        FlatProto2OuterClass.FlatProto2 in =
                FlatProto2OuterClass.FlatProto2.newBuilder()
                        .setString(TEST_STRING)
                        .setInt(TEST_INT)
                        .setLong(TEST_LONG)
                        .setFloat(TEST_FLOAT)
                        .setDouble(TEST_DOUBLE)
                        .addInts(TEST_INT)
                        .setBytes(TEST_BYTES)
                        .setBool(TEST_BOOL)
                        .build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void flatProto2WithFieldsOmitted() throws Exception {
        FlatProto2OuterClass.FlatProto2 in =
                FlatProto2OuterClass.FlatProto2.newBuilder()
                        .setLong(TEST_LONG)
                        .setFloat(TEST_FLOAT)
                        .setDouble(TEST_DOUBLE)
                        .addInts(TEST_INT)
                        .setBool(TEST_BOOL)
                        .build();
        runCompilerTest(in, DEFAULT_SCHEMA_ID);
    }

    @Test
    public void sameSchemaDifferentIds() throws Exception {
        FlatProto3OuterClass.FlatProto3 in =
                FlatProto3OuterClass.FlatProto3.newBuilder()
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

        ProtobufSchema schema = new ProtobufSchema(in.getDescriptorForType());
        ProtoCompiler protoCompiler =
                new ProtoCompiler(DEFAULT_CLASS_SUFFIX, USE_DEFAULT_PROTO_INCLUDES);
        Class generatedClass1 = protoCompiler.generateMessageClass(schema, 1);
        Class generatedClass2 = protoCompiler.generateMessageClass(schema, 2);

        byte[] reprocessedBytes1 = processOriginalBytesWithGeneratedClass(generatedClass1, inBytes);
        byte[] reprocessedBytes2 = processOriginalBytesWithGeneratedClass(generatedClass2, inBytes);

        Assertions.assertArrayEquals(
                inBytes, reprocessedBytes1, "Input and output1 messages are not the same");
        Assertions.assertArrayEquals(
                inBytes, reprocessedBytes2, "Input and output2 messages are not the same");

        assertGeneratedClassName(in, generatedClass1, 1);
        assertGeneratedClassName(in, generatedClass2, 2);
    }

    @Test
    public void noClassNameClashForSameSchemaOnDifferentThreads() throws Exception {
        FlatProto3OuterClass.FlatProto3 in =
                FlatProto3OuterClass.FlatProto3.newBuilder()
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
        ProtobufSchema schema = new ProtobufSchema(in.getDescriptorForType());

        Queue<Class> classes = new ConcurrentLinkedQueue<>();

        Thread thread1 =
                new Thread(
                        () -> {
                            try {
                                ProtoCompiler protoCompiler =
                                        new ProtoCompiler(USE_DEFAULT_PROTO_INCLUDES);
                                Class generatedClass =
                                        protoCompiler.generateMessageClass(
                                                schema, DEFAULT_SCHEMA_ID);
                                classes.add(generatedClass);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        Thread thread2 =
                new Thread(
                        () -> {
                            try {
                                ProtoCompiler protoCompiler =
                                        new ProtoCompiler(USE_DEFAULT_PROTO_INCLUDES);
                                Class generatedClass =
                                        protoCompiler.generateMessageClass(
                                                schema, DEFAULT_SCHEMA_ID);
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
                inBytes, reprocessedBytes1, "Input and output1 messages are not the same");
        Assertions.assertArrayEquals(
                inBytes, reprocessedBytes2, "Input and output2 messages are not the same");

        Assertions.assertNotEquals(generatedClass1.getName(), generatedClass2.getName());
    }

    private Class runCompilerTest(Message message, int fakeSchemaId) throws Exception {
        byte[] inBytes = message.toByteArray();

        // Fixtures
        ProtobufSchema schema = new ProtobufSchema(message.getDescriptorForType());
        ProtoCompiler protoCompiler =
                new ProtoCompiler(DEFAULT_CLASS_SUFFIX, USE_DEFAULT_PROTO_INCLUDES);

        // Call the target method
        Class messageClass = protoCompiler.generateMessageClass(schema, fakeSchemaId);

        // Parse the original message and then serialize it back
        byte[] outBytes = processOriginalBytesWithGeneratedClass(messageClass, inBytes);

        // Check if the original and the rebuilt messages are the same
        Assertions.assertArrayEquals(
                inBytes, outBytes, "Input and output messages are not the same");
        return messageClass;
    }

    private byte[] processOriginalBytesWithGeneratedClass(Class generatedClass, byte[] inBytes)
            throws Exception {
        Method parseFromMethod =
                generatedClass.getMethod(PbConstant.PB_METHOD_PARSE_FROM, byte[].class);
        Method toByteArrayMethod = generatedClass.getMethod("toByteArray");

        // Parse the original message and then serialize it back
        return (byte[]) toByteArrayMethod.invoke(parseFromMethod.invoke(null, inBytes));
    }

    public void assertGeneratedClassName(
            Message originalMessage, Class generatedClass, int fakeSchemaId) {
        String originalMessageClassName = originalMessage.getClass().getName();
        String[] originalMessageClassNameParts = originalMessageClassName.split("\\$");
        String originalMessageInnerClassName =
                originalMessageClassNameParts[originalMessageClassNameParts.length - 1];
        String generatedClassName = generatedClass.getName();
        String expectedGeneratedClassName =
                String.format(
                        "%s.%s_%d_%s$%s",
                        DEFAULT_PACKAGE,
                        originalMessageInnerClassName,
                        fakeSchemaId,
                        DEFAULT_CLASS_SUFFIX,
                        originalMessageInnerClassName);
        Assertions.assertEquals(expectedGeneratedClassName, generatedClassName);
    }
}
