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

package org.apache.flink.streaming.examples.statemachine;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.examples.statemachine.generator.EventsGeneratorSource;
import org.apache.flink.streaming.examples.statemachine.kafka.EventDeSerializer;

/**
 * Job to generate input events that are written to Kafka, for the {@link StateMachineExample} job.
 */
public class KafkaEventsGeneratorJob {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        double errorRate = params.getDouble("error-rate", 0.0);
        int sleep = params.getInt("sleep", 1);

        String kafkaTopic = params.get("kafka-topic");
        String brokers = params.get("brokers", "localhost:9092");

        System.out.printf(
                "Generating events to Kafka with standalone source with error rate %f and sleep delay %s millis\n",
                errorRate, sleep);
        System.out.println();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new EventsGeneratorSource(errorRate, sleep))
                .addSink(new FlinkKafkaProducer<>(brokers, kafkaTopic, new EventDeSerializer()));

        // trigger program execution
        env.execute("State machine example Kafka events generator job");
    }
}
