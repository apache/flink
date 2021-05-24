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

package org.apache.flink.streaming.examples.gcp.pubsub;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple PubSub example.
 *
 * <p>Before starting a flink job it will publish 10 messages on the input topic.
 *
 * <p>Then a flink job is started to read these 10 messages from the input-subscription, it will
 * print them to stdout and then write them to a the output-topic.
 */
public class PubSubExample {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubExample.class);

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 3) {
            System.out.println(
                    "Missing parameters!\n"
                            + "Usage: flink run PubSub.jar --input-subscription <subscription> --input-topicName <topic> --output-topicName <output-topic> "
                            + "--google-project <google project name> ");
            return;
        }

        String projectName = parameterTool.getRequired("google-project");
        String inputTopicName = parameterTool.getRequired("input-topicName");
        String subscriptionName = parameterTool.getRequired("input-subscription");
        String outputTopicName = parameterTool.getRequired("output-topicName");

        PubSubPublisher pubSubPublisher = new PubSubPublisher(projectName, inputTopicName);
        pubSubPublisher.publish(10);

        runFlinkJob(projectName, subscriptionName, outputTopicName);
    }

    private static void runFlinkJob(
            String projectName, String subscriptionName, String outputTopicName) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        env.addSource(
                        PubSubSource.newBuilder()
                                .withDeserializationSchema(new IntegerSerializer())
                                .withProjectName(projectName)
                                .withSubscriptionName(subscriptionName)
                                .withMessageRateLimit(1)
                                .build())
                .map(PubSubExample::printAndReturn)
                .disableChaining()
                .addSink(
                        PubSubSink.newBuilder()
                                .withSerializationSchema(new IntegerSerializer())
                                .withProjectName(projectName)
                                .withTopicName(outputTopicName)
                                .build());

        env.execute("Flink Streaming PubSubReader");
    }

    private static Integer printAndReturn(Integer i) {
        LOG.info("Processed message with payload: " + i);
        return i;
    }
}
