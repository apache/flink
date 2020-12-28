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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.test.util.TestUtils.tryExecute;

/**
 * A thread that runs a topology with the FlinkKinesisConsumer as source, followed by two flat map
 * functions, one that performs artificial failures and another that validates exactly-once
 * guarantee.
 */
public class ExactlyOnceValidatingConsumerThread {

    private static final Logger LOG =
            LoggerFactory.getLogger(ExactlyOnceValidatingConsumerThread.class);

    public static Thread create(
            final int totalEventCount,
            final int failAtRecordCount,
            final int parallelism,
            final int checkpointInterval,
            final long restartDelay,
            final String awsAccessKey,
            final String awsSecretKey,
            final String awsRegion,
            final String kinesisStreamName,
            final AtomicReference<Throwable> errorHandler,
            final int flinkPort,
            final Configuration flinkConfig) {
        Runnable exactlyOnceValidationConsumer =
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            StreamExecutionEnvironment see =
                                    StreamExecutionEnvironment.createRemoteEnvironment(
                                            "localhost", flinkPort, flinkConfig);
                            see.setParallelism(parallelism);
                            see.enableCheckpointing(checkpointInterval);
                            // we restart two times
                            see.setRestartStrategy(
                                    RestartStrategies.fixedDelayRestart(2, restartDelay));

                            // consuming topology
                            Properties consumerProps = new Properties();
                            consumerProps.setProperty(
                                    ConsumerConfigConstants.AWS_ACCESS_KEY_ID, awsAccessKey);
                            consumerProps.setProperty(
                                    ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, awsSecretKey);
                            consumerProps.setProperty(
                                    ConsumerConfigConstants.AWS_REGION, awsRegion);
                            // start reading from beginning
                            consumerProps.setProperty(
                                    ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                                    ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.name());
                            DataStream<String> consuming =
                                    see.addSource(
                                            new FlinkKinesisConsumer<>(
                                                    kinesisStreamName,
                                                    new SimpleStringSchema(),
                                                    consumerProps));
                            consuming
                                    .flatMap(new ArtificialFailOnceFlatMapper(failAtRecordCount))
                                    // validate consumed records for correctness (use only 1
                                    // instance to validate all consumed records)
                                    .flatMap(new ExactlyOnceValidatingMapper(totalEventCount))
                                    .setParallelism(1);

                            LOG.info("Starting consuming topology");
                            tryExecute(see, "Consuming topo");
                            LOG.info("Consuming topo finished");
                        } catch (Exception e) {
                            LOG.warn("Error while running consuming topology", e);
                            errorHandler.set(e);
                        }
                    }
                };

        return new Thread(exactlyOnceValidationConsumer);
    }

    private static class ExactlyOnceValidatingMapper
            implements FlatMapFunction<String, String>, ListCheckpointed<BitSet> {

        private static final Logger LOG =
                LoggerFactory.getLogger(ExactlyOnceValidatingMapper.class);

        private final int totalEventCount;
        private BitSet validator;

        public ExactlyOnceValidatingMapper(int totalEventCount) {
            this.totalEventCount = totalEventCount;
            this.validator = new BitSet(totalEventCount);
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            LOG.info("Consumed {}", value);

            int id = Integer.parseInt(value.split("-")[0]);
            if (validator.get(id)) {
                throw new RuntimeException("Saw id " + id + " twice!");
            }
            validator.set(id);
            if (id > totalEventCount - 1) {
                throw new RuntimeException("Out of bounds ID observed");
            }

            if (validator.nextClearBit(0) == totalEventCount) {
                throw new SuccessException();
            }
        }

        @Override
        public List<BitSet> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(validator);
        }

        @Override
        public void restoreState(List<BitSet> state) throws Exception {
            // we expect either 1 or 0 elements
            if (state.size() == 1) {
                validator = state.get(0);
            } else {
                Preconditions.checkState(state.isEmpty());
            }
        }
    }

    private static class ArtificialFailOnceFlatMapper extends RichFlatMapFunction<String, String> {
        int count = 0;

        private final int failAtRecordCount;

        public ArtificialFailOnceFlatMapper(int failAtRecordCount) {
            this.failAtRecordCount = failAtRecordCount;
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (count++ >= failAtRecordCount && getRuntimeContext().getAttemptNumber() == 0) {
                throw new RuntimeException("Artificial failure. Restart please.");
            }
            out.collect(value);
        }
    }
}
