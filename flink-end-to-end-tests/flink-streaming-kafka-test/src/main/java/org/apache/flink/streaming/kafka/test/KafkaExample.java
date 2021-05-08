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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.kafka.test.base.CustomWatermarkStrategy;
import org.apache.flink.streaming.kafka.test.base.KafkaEvent;
import org.apache.flink.streaming.kafka.test.base.KafkaEventSchema;
import org.apache.flink.streaming.kafka.test.base.KafkaExampleUtil;
import org.apache.flink.streaming.kafka.test.base.RollingAdditionMapper;

import java.util.Properties;

/**
 * A simple example that shows how to read from and write to modern Kafka. This will read String
 * messages from the input topic, parse them into a POJO type {@link KafkaEvent}, group by some key,
 * and finally perform a rolling addition on each key for which the results are written back to
 * another topic.
 *
 * <p>This example also demonstrates using a watermark assigner to generate per-partition watermarks
 * directly in the Flink Kafka consumer. For demonstration purposes, it is assumed that the String
 * messages are of formatted as a (word,frequency,timestamp) tuple.
 *
 * <p>Example usage: --input-topic test-input --output-topic test-output --bootstrap.servers
 * localhost:9092 --group.id myconsumer --partition-discovery-interval-ms 60000
 */
public class KafkaExample extends KafkaExampleUtil {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = KafkaExampleUtil.prepareExecutionEnv(parameterTool);

        // Whether to use Kafka Source based on FLIP-27 Source API
        boolean useNewApi = parameterTool.has("use-new-source");

        // Input and output topic name
        String inputTopic = parameterTool.getRequired("input-topic");
        String outputTopic = parameterTool.getRequired("output-topic");

        // Additional Kafka client properties
        Properties properties = parameterTool.getProperties();

        // The option for setting partition discovery interval is different in the old and new
        // source. We use this helper function to "translate" the option to use the correct one in
        // different sources.
        setPartitionDiscoveryInterval(properties, useNewApi);

        DataStream<KafkaEvent> input;

        if (useNewApi) {
            // Kafka FLIP-27 Source
            final KafkaSource<KafkaEvent> source =
                    KafkaSource.<KafkaEvent>builder()
                            .setTopics(inputTopic)
                            .setValueOnlyDeserializer(new KafkaEventSchema())
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();
            input = env.fromSource(source, new CustomWatermarkStrategy(), "Kafka Source");
        } else {
            // Kafka SourceFunction
            final FlinkKafkaConsumerBase<KafkaEvent> sourceFunction =
                    new FlinkKafkaConsumer<>(inputTopic, new KafkaEventSchema(), properties)
                            .setStartFromEarliest();
            input =
                    env.addSource(sourceFunction)
                            .name("Kafka SourceFunction")
                            .assignTimestampsAndWatermarks(new CustomWatermarkStrategy());
        }

        input.keyBy((KeySelector<KafkaEvent, String>) KafkaEvent::getWord)
                .map(new RollingAdditionMapper())
                .addSink(
                        new FlinkKafkaProducer<>(
                                outputTopic,
                                new KafkaSerializationSchemaWrapper<>(
                                        outputTopic, null, false, new KafkaEventSchema()),
                                properties,
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("Kafka SinkFunction");

        env.execute("Modern Kafka Example");
    }

    private static void setPartitionDiscoveryInterval(Properties properties, boolean useNewApi) {
        if (properties == null) {
            throw new NullPointerException("Kafka properties should not be null");
        }

        String partitionDiscoveryIntervalMs =
                properties.getProperty("partition-discovery-interval-ms", "-1");

        if (useNewApi) {
            properties.setProperty(
                    KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                    partitionDiscoveryIntervalMs);
        } else {
            properties.setProperty(
                    FlinkKafkaConsumer.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                    partitionDiscoveryIntervalMs);
        }
    }
}
