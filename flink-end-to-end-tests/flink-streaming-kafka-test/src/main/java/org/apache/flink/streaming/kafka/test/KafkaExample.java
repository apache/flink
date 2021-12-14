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

package org.apache.flink.streaming.kafka.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.kafka.test.base.KafkaExampleUtil;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

/**
 * A simple application used as smoke test example to forward messages from one topic to another
 * topic in batch mode.
 *
 * <p>Example usage: --input-topic test-input --output-topic test-output --bootstrap.servers
 * localhost:9092 --group.id myconsumer
 */
public class KafkaExample extends KafkaExampleUtil {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = KafkaExampleUtil.prepareExecutionEnv(parameterTool);

        DataStream<Integer> input =
                env.fromSource(
                        KafkaSource.<Integer>builder()
                                .setBootstrapServers(
                                        parameterTool
                                                .getProperties()
                                                .getProperty(
                                                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                                .setBounded(OffsetsInitializer.latest())
                                .setDeserializer(
                                        KafkaRecordDeserializationSchema.valueOnly(
                                                IntegerDeserializer.class))
                                .setTopics(parameterTool.getRequired("input-topic"))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source");

        input.sinkTo(
                KafkaSink.<Integer>builder()
                        .setBootstrapServers(
                                parameterTool
                                        .getProperties()
                                        .getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(parameterTool.getRequired("output-topic"))
                                        .setKafkaValueSerializer(IntegerSerializer.class)
                                        .build())
                        .build());
        env.execute("Smoke Kafka Example");
    }
}
