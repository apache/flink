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

package org.apache.flink.protobuf.registry.confluent;

import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

    public static final String TEST_STRING = "test";
    public static final int TEST_INT = 42;
    public static final long TEST_LONG = 99L;
    public static final float TEST_FLOAT = 3.14f;
    public static final double TEST_DOUBLE = 2.71828;
    public static final boolean TEST_BOOL = true;
    public static final ByteString TEST_BYTES =
            ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03, 0x04});

    public static final String STRING_FIELD = "string";
    public static final String INT_FIELD = "int";
    public static final String LONG_FIELD = "long";
    public static final String FLOAT_FIELD = "float";
    public static final String DOUBLE_FIELD = "double";
    public static final String BOOL_FIELD = "bool";
    public static final String BYTES_FIELD = "bytes";
    public static final String NESTED_FIELD = "nested";
    public static final String ARRAY_FIELD = "array";
    public static final String MAP_FIELD = "map";
    public static final String TIMESTAMP_FIELD = "ts";
    public static final String SECONDS_FIELD = "seconds";
    public static final String NANOS_FIELD = "nanos";

    public static final String DEFAULT_PACKAGE = "org.apache.flink.formats.protobuf.proto";
    public static final int DEFAULT_SCHEMA_ID = 1;
    public static final String DEFAULT_CLASS_SUFFIX = "123";
    public static final String DEFAULT_CLASS_NAME = "TestClass";
    public static final String DUMMY_SCHEMA_REGISTRY_URL = "http://registry:8081";
    public static final String FAKE_TOPIC = "fake-topic";
    public static final String FAKE_SUBJECT = "fake-subject";
    public static final boolean IGNORE_PARSE_ERRORS = false;
    public static final boolean READ_DEFAULT_VALUES = false;

    public static RowType createRowType(RowType.RowField... fields) {
        List<RowType.RowField> fieldList = new ArrayList<>();
        fieldList.addAll(Arrays.asList(fields));
        return new RowType(fieldList);
    }

    public static Message parseBytesToMessage(
            byte[] bytes, SchemaRegistryClient mockSchemaRegistryClient) {
        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", DUMMY_SCHEMA_REGISTRY_URL);
        KafkaProtobufDeserializer deser =
                new KafkaProtobufDeserializer(mockSchemaRegistryClient, opts);
        return deser.deserialize(null, bytes);
    }
}
