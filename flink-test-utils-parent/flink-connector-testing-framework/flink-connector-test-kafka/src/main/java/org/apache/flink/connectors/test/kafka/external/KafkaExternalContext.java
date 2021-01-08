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

package org.apache.flink.connectors.test.kafka.external;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceJobTerminationPattern;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/** Context for Kafka tests. */
public class KafkaExternalContext implements ExternalContext<String> {

    private final Properties kafkaProperties;

    public KafkaExternalContext(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public String jobName() {
        return "KafkaConnectorTest";
    }

    @Override
    public SourceFunction<String> createSource() {
        FlinkKafkaConsumer<String> kafkaSource =
                new FlinkKafkaConsumer<>(
                        KafkaContainerizedExternalSystem.TOPIC,
                        new SimpleStringSchema() {
                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return nextElement.equals("END");
                            }
                        },
                        kafkaProperties);
        kafkaSource.setStartFromEarliest();
        return kafkaSource;
    }

    @Override
    public SinkFunction<String> createSink() {
        return new FlinkKafkaProducer<>(
                KafkaContainerizedExternalSystem.TOPIC, new SimpleStringSchema(), kafkaProperties);
    }

    @Override
    public SourceJobTerminationPattern sourceJobTerminationPattern() {
        return SourceJobTerminationPattern.DESERIALIZATION_SCHEMA;
    }
}
