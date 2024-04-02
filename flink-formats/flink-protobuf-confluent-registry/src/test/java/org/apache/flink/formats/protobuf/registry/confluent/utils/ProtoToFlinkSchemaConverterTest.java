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

package org.apache.flink.formats.protobuf.registry.confluent.utils;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProtoToFlinkSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
public class ProtoToFlinkSchemaConverterTest {

    public static final VarCharType STRING_NOT_NULL_TYPE =
            new VarCharType(false, VarCharType.MAX_LENGTH);
    private static final CommonMappings.TypeMapping CUSTOM_SCHEMA_CASE =
            new CommonMappings.TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "\n"
                            + "package foo;\n"
                            + "\n"
                            + "message Customer {\n"
                            + "    int64 count = 1;\n"
                            + "    string first_name = 2;\n"
                            + "    string last_name = 3;\n"
                            + "    string address = 4;\n"
                            + "    oneof survey_id {\n"
                            + "        string survey_response_id = 5;\n"
                            + "        string outgoing_action_id = 6;\n"
                            + "    }\n"
                            + "    oneof comment {\n"
                            + "        string email = 7;\n"
                            + "        string platform = 8;\n"
                            + "    }\n"
                            + "}",
                    new RowType(
                            false,
                            asList(
                                    new RowField("count", new BigIntType(false)),
                                    new RowField("first_name", STRING_NOT_NULL_TYPE),
                                    new RowField("last_name", STRING_NOT_NULL_TYPE),
                                    new RowField("address", STRING_NOT_NULL_TYPE),
                                    new RowField(
                                            "survey_id",
                                            new RowType(
                                                    Arrays.asList(
                                                            new RowField(
                                                                    "survey_response_id",
                                                                    STRING_NOT_NULL_TYPE),
                                                            new RowField(
                                                                    "outgoing_action_id",
                                                                    STRING_NOT_NULL_TYPE)))),
                                    new RowField(
                                            "comment",
                                            new RowType(
                                                    Arrays.asList(
                                                            new RowField(
                                                                    "email", STRING_NOT_NULL_TYPE),
                                                            new RowField(
                                                                    "platform",
                                                                    STRING_NOT_NULL_TYPE)))))));
    private static final CommonMappings.TypeMapping WRAPPERS_CASE =
            new CommonMappings.TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "import \"google/protobuf/wrappers.proto\";"
                            + "\n"
                            + "message Row {\n"
                            + "  google.protobuf.StringValue string = 1;\n"
                            + "  google.protobuf.Int32Value int = 3;\n"
                            + "  google.protobuf.Int64Value bigint = 4;\n"
                            + "  google.protobuf.FloatValue float = 5;\n"
                            + "  google.protobuf.DoubleValue double = 6;\n"
                            + "  google.protobuf.BoolValue boolean = 7;\n"
                            + "}\n",
                    new RowType(
                            false,
                            asList(
                                    new RowField(
                                            "string",
                                            new VarCharType(true, VarCharType.MAX_LENGTH)),
                                    new RowField("int", new IntType(true)),
                                    new RowField("bigint", new BigIntType(true)),
                                    new RowField("float", new FloatType(true)),
                                    new RowField("double", new DoubleType(true)),
                                    new RowField("boolean", new BooleanType(true)))));

    public static Stream<Arguments> typesToCheck() {
        return Stream.concat(CommonMappings.get(), Stream.of(WRAPPERS_CASE, CUSTOM_SCHEMA_CASE))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(CommonMappings.TypeMapping mapping) {
        assertThat(ProtoToFlinkSchemaConverter.toFlinkSchema(mapping.getProtoSchema()))
                .isEqualTo(mapping.getFlinkType());
    }

    @Test
    void testRecursiveSchema() {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "\n"
                        + "option java_package = \"io.confluent.connect.protobuf.test\";\n"
                        + "option java_outer_classname = \"RecursiveKeyValue\";\n"
                        + "\n"
                        + "import \"google/protobuf/descriptor.proto\";\n"
                        + "\n"
                        + "message RecursiveKeyValueMessage {\n"
                        + "    int32 key = 1;\n"
                        + "    string value = 2;\n"
                        + "    RecursiveKeyValueMessage key_value = 10;\n"
                        + "}";
        assertThatThrownBy(
                        () ->
                                ProtoToFlinkSchemaConverter.toFlinkSchema(
                                        new ProtobufSchema(schemaStr).toDescriptor()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cyclic schemas are not supported.");
    }
}
