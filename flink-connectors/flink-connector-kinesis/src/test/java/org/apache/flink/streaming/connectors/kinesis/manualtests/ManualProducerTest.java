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

package org.apache.flink.streaming.connectors.kinesis.manualtests;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.KinesisPartitioner;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.examples.ProduceIntoKinesis;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;

import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * This is a manual test for the AWS Kinesis connector in Flink.
 *
 * <p>It uses: - A custom KinesisSerializationSchema - A custom KinesisPartitioner
 *
 * <p>The streams "test-flink" and "flink-test-2" must exist.
 *
 * <p>Invocation: --region eu-central-1 --accessKey X --secretKey X
 */
public class ManualProducerTest {

    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(4);

        DataStream<String> simpleStringStream =
                see.addSource(new ProduceIntoKinesis.EventsGenerator());

        Properties kinesisProducerConfig = new Properties();
        kinesisProducerConfig.setProperty(AWSConfigConstants.AWS_REGION, pt.getRequired("region"));
        kinesisProducerConfig.setProperty(
                AWSConfigConstants.AWS_ACCESS_KEY_ID, pt.getRequired("accessKey"));
        kinesisProducerConfig.setProperty(
                AWSConfigConstants.AWS_SECRET_ACCESS_KEY, pt.getRequired("secretKey"));

        FlinkKinesisProducer<String> kinesis =
                new FlinkKinesisProducer<>(
                        new KinesisSerializationSchema<String>() {
                            @Override
                            public ByteBuffer serialize(String element) {
                                return ByteBuffer.wrap(
                                        element.getBytes(ConfigConstants.DEFAULT_CHARSET));
                            }

                            // every 10th element goes into a different stream
                            @Override
                            public String getTargetStream(String element) {
                                if (element.split("-")[0].endsWith("0")) {
                                    return "flink-test-2";
                                }
                                return null; // send to default stream
                            }
                        },
                        kinesisProducerConfig);

        kinesis.setFailOnError(true);
        kinesis.setDefaultStream("test-flink");
        kinesis.setDefaultPartition("0");
        kinesis.setCustomPartitioner(
                new KinesisPartitioner<String>() {
                    @Override
                    public String getPartitionId(String element) {
                        int l = element.length();
                        return element.substring(l - 1, l);
                    }
                });
        simpleStringStream.addSink(kinesis);

        see.execute();
    }
}
