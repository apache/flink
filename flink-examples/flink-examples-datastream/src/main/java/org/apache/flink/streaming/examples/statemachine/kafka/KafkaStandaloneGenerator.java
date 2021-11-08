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

package org.apache.flink.streaming.examples.statemachine.kafka;

import org.apache.flink.streaming.examples.statemachine.event.Event;
import org.apache.flink.streaming.examples.statemachine.generator.StandaloneThreadedGenerator;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A generator that pushes the data into Kafka. */
public class KafkaStandaloneGenerator extends StandaloneThreadedGenerator {

    public static final String BROKER_ADDRESS = "localhost:9092";

    public static final String TOPIC = "flink-demo-topic-1";

    public static final int NUM_PARTITIONS = 1;

    /** Entry point to the kafka data producer. */
    public static void main(String[] args) throws Exception {

        final KafkaCollector[] collectors = new KafkaCollector[NUM_PARTITIONS];

        // create the generator threads
        for (int i = 0; i < collectors.length; i++) {
            collectors[i] = new KafkaCollector(BROKER_ADDRESS, TOPIC, i);
        }

        StandaloneThreadedGenerator.runGenerator(collectors);
    }

    // ------------------------------------------------------------------------

    private static class KafkaCollector implements Collector<Event>, AutoCloseable {

        private final KafkaProducer<Object, byte[]> producer;

        private final EventDeSerializationSchema serializer;

        private final String topic;

        private final int partition;

        KafkaCollector(String brokerAddress, String topic, int partition) {
            this.topic = checkNotNull(topic);
            this.partition = partition;
            this.serializer = new EventDeSerializationSchema();

            // create Kafka producer
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
            properties.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getCanonicalName());
            properties.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getCanonicalName());
            this.producer = new KafkaProducer<>(properties);
        }

        @Override
        public void collect(Event evt) {
            byte[] serialized = serializer.serialize(evt);
            producer.send(new ProducerRecord<>(topic, partition, null, serialized));
        }

        @Override
        public void close() {
            producer.close();
        }
    }
}
