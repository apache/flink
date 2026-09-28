/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.schema.registry.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.upserttest.sink.UpsertTestSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Reads Confluent-framed Avro records, one base64-encoded record per line, with the Confluent
 * Schema Registry deserialization schemas and writes them to files with the registry serialization
 * schema. Used by {@code ConfluentSchemaRegistryITCase}; outputs are keyed by the user name:
 *
 * <ul>
 *   <li>{@code string.out}: each {@link User} as a string,
 *   <li>{@code avro.out}: each {@link User} serialized under {@code --output-subject},
 *   <li>{@code evolved.out}: each record read with {@link #evolvedUserSchema()} as a string.
 * </ul>
 */
public class TestAvroConsumerConfluent {

    public static final String STRING_OUTPUT = "string.out";
    public static final String AVRO_OUTPUT = "avro.out";
    public static final String EVOLVED_OUTPUT = "evolved.out";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String inputPath = params.getRequired("input-path");
        final String outputDir = params.getRequired("output-dir");
        final String registryUrl = params.getRequired("schema-registry-url");
        final String outputSubject = params.getRequired("output-subject");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // UpsertTestSink writes a single file per sink
        env.setParallelism(1);

        final DataStream<byte[]> input =
                env.fromSource(
                                FileSource.forRecordStreamFormat(
                                                new TextLineInputFormat(), new Path(inputPath))
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "input")
                        .map(line -> Base64.getDecoder().decode(line))
                        .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

        final DataStream<User> users =
                input.map(
                        new Deserialize<>(
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                        User.class, registryUrl)));

        users.sinkTo(
                UpsertTestSink.<User>builder()
                        .setOutputFile(new File(outputDir, STRING_OUTPUT))
                        .setKeySerializationSchema(user -> utf8(user.getName()))
                        .setValueSerializationSchema(user -> utf8(user))
                        .build());

        users.sinkTo(
                UpsertTestSink.<User>builder()
                        .setOutputFile(new File(outputDir, AVRO_OUTPUT))
                        .setKeySerializationSchema(user -> utf8(user.getName()))
                        .setValueSerializationSchema(
                                ConfluentRegistryAvroSerializationSchema.forSpecific(
                                        User.class, outputSubject, registryUrl))
                        .build());

        input.map(
                        new Deserialize<>(
                                ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                        evolvedUserSchema(), registryUrl)))
                .sinkTo(
                        UpsertTestSink.<GenericRecord>builder()
                                .setOutputFile(new File(outputDir, EVOLVED_OUTPUT))
                                .setKeySerializationSchema(record -> utf8(record.get("name")))
                                .setValueSerializationSchema(record -> utf8(record))
                                .build());

        env.execute("Confluent Schema Registry end-to-end test");
    }

    /** The {@link User} schema plus a {@code department} field with a default value. */
    public static Schema evolvedUserSchema() {
        final Schema user = User.getClassSchema();
        final List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field field : user.getFields()) {
            fields.add(new Schema.Field(field, field.schema()));
        }
        fields.add(
                new Schema.Field("department", Schema.create(Schema.Type.STRING), null, "unknown"));
        return Schema.createRecord(user.getName(), null, user.getNamespace(), false, fields);
    }

    // UpsertTestFileUtil stores key and value lengths in one byte, so records must stay below
    // 256 bytes.
    private static byte[] utf8(Object value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Deserializes each record the same way a source applies its {@link DeserializationSchema}. */
    private static final class Deserialize<T>
            implements MapFunction<byte[], T>, ResultTypeQueryable<T> {

        private final DeserializationSchema<T> schema;

        private Deserialize(DeserializationSchema<T> schema) {
            this.schema = schema;
        }

        @Override
        public T map(byte[] bytes) throws Exception {
            return schema.deserialize(bytes);
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return schema.getProducedType();
        }
    }
}
