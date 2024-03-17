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

import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkToProtoSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
class FlinkToProtoSchemaConverterTest {

    private static final CommonMappings.TypeMapping MULTISET_CASE =
            new CommonMappings.TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "message Row {\n"
                            + "  repeated MultisetEntry multiset = 1;\n"
                            + "\n"
                            + "  message MultisetEntry {\n"
                            + "    optional string key = 1;\n"
                            + "    int32 value = 2;\n"
                            + "  }\n"
                            + "}",
                    new RowType(
                            false,
                            Collections.singletonList(
                                    new RowField(
                                            "multiset",
                                            new MultisetType(
                                                    true,
                                                    new VarCharType(
                                                            true, VarCharType.MAX_LENGTH))))));

    public static Stream<Arguments> typesToCheck() {
        return Stream.concat(CommonMappings.get(), Stream.of(MULTISET_CASE)).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(CommonMappings.TypeMapping mapping) {
        final Descriptor descriptor =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        (RowType) mapping.getFlinkType(), "Row", "io.confluent.protobuf.generated");

        assertThat(new ProtobufSchema(descriptor).toString().trim())
                .isEqualTo(mapping.getExpectedString().trim());
    }
}
