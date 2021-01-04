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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread that runs a topology with a manual data generator as source, and the
 * FlinkKinesisProducer as sink.
 */
public class KinesisEventsGeneratorProducerThread {

    private static final Logger LOG =
            LoggerFactory.getLogger(KinesisEventsGeneratorProducerThread.class);

    public static Thread create(
            final int totalEventCount,
            final int parallelism,
            final String awsAccessKey,
            final String awsSecretKey,
            final String awsRegion,
            final String kinesisStreamName,
            final AtomicReference<Throwable> errorHandler,
            final int flinkPort,
            final Configuration flinkConfig) {
        Runnable kinesisEventsGeneratorProducer =
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            StreamExecutionEnvironment see =
                                    StreamExecutionEnvironment.createRemoteEnvironment(
                                            "localhost", flinkPort, flinkConfig);
                            see.setParallelism(parallelism);

                            // start data generator
                            DataStream<String> simpleStringStream =
                                    see.addSource(
                                                    new KinesisEventsGeneratorProducerThread
                                                            .EventsGenerator(totalEventCount))
                                            .setParallelism(1);

                            Properties producerProps = new Properties();
                            producerProps.setProperty(
                                    AWSConfigConstants.AWS_ACCESS_KEY_ID, awsAccessKey);
                            producerProps.setProperty(
                                    AWSConfigConstants.AWS_SECRET_ACCESS_KEY, awsSecretKey);
                            producerProps.setProperty(AWSConfigConstants.AWS_REGION, awsRegion);

                            FlinkKinesisProducer<String> kinesis =
                                    new FlinkKinesisProducer<>(
                                            new SimpleStringSchema(), producerProps);

                            kinesis.setFailOnError(true);
                            kinesis.setDefaultStream(kinesisStreamName);
                            kinesis.setDefaultPartition("0");
                            simpleStringStream.addSink(kinesis);

                            LOG.info("Starting producing topology");
                            see.execute("Producing topology");
                            LOG.info("Producing topo finished");
                        } catch (Exception e) {
                            LOG.warn("Error while running producing topology", e);
                            errorHandler.set(e);
                        }
                    }
                };

        return new Thread(kinesisEventsGeneratorProducer);
    }

    private static class EventsGenerator implements SourceFunction<String> {

        private static final Logger LOG = LoggerFactory.getLogger(EventsGenerator.class);

        private boolean running = true;
        private final long limit;

        public EventsGenerator(long limit) {
            this.limit = limit;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long seq = 0;
            while (running) {
                Thread.sleep(10);
                String evt = (seq++) + "-" + RandomStringUtils.randomAlphabetic(12);
                ctx.collect(evt);
                LOG.info("Emitting event {}", evt);
                if (seq >= limit) {
                    break;
                }
            }
            ctx.close();
            LOG.info("Stopping events generator");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
