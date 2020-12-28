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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import example.avro.User;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Properties;

/**
 * A simple example that shows how to read from and write to Kafka with Confluent Schema Registry.
 * This will read AVRO messages from the input topic, parse them into a POJO type via checking the
 * Schema by calling Schema registry. Then this example publish the POJO type to kafka by converting
 * the POJO to AVRO and verifying the schema. --input-topic test-input --output-string-topic
 * test-output --output-avro-topic test-avro-output --output-subject --bootstrap.servers
 * localhost:9092 --schema-registry-url http://localhost:8081 --group.id myconsumer
 */
public class TestAvroConsumerConfluent {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 6) {
            System.out.println(
                    "Missing parameters!\n"
                            + "Usage: Kafka --input-topic <topic> --output-string-topic <topic> --output-avro-topic <topic> "
                            + "--bootstrap.servers <kafka brokers> "
                            + "--schema-registry-url <confluent schema registry> --group.id <some id>");
            return;
        }
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        config.setProperty("group.id", parameterTool.getRequired("group.id"));
        String schemaRegistryUrl = parameterTool.getRequired("schema-registry-url");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<User> input =
                env.addSource(
                        new FlinkKafkaConsumer<>(
                                        parameterTool.getRequired("input-topic"),
                                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                                User.class, schemaRegistryUrl),
                                        config)
                                .setStartFromEarliest());

        SingleOutputStreamOperator<String> mapToString =
                input.map((MapFunction<User, String>) SpecificRecordBase::toString);

        FlinkKafkaProducer<String> stringFlinkKafkaProducer =
                new FlinkKafkaProducer<>(
                        parameterTool.getRequired("output-string-topic"),
                        new SimpleStringSchema(),
                        config);
        mapToString.addSink(stringFlinkKafkaProducer);

        FlinkKafkaProducer<User> avroFlinkKafkaProducer =
                new FlinkKafkaProducer<>(
                        parameterTool.getRequired("output-avro-topic"),
                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                                User.class,
                                parameterTool.getRequired("output-subject"),
                                schemaRegistryUrl),
                        config);
        input.addSink(avroFlinkKafkaProducer);

        env.execute("Kafka Confluent Schema Registry AVRO Example");
    }
}
