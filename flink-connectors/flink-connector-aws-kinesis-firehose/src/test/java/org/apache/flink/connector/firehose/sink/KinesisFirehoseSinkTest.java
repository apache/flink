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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;

/** Covers construction, defaults and sanity checking of {@link KinesisFirehoseSink}. */
public class KinesisFirehoseSinkTest {

    private static final ElementConverter<String, Record> elementConverter =
            KinesisFirehoseSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @Test
    public void deliveryStreamNameMustNotBeNull() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                new KinesisFirehoseSink<>(
                                        elementConverter,
                                        500,
                                        16,
                                        10000,
                                        4 * 1024 * 1024L,
                                        5000L,
                                        1000 * 1024L,
                                        false,
                                        null,
                                        new Properties()))
                .withMessageContaining(
                        "The delivery stream name must not be null when initializing the KDF Sink.");
    }

    @Test
    public void deliveryStreamNameMustNotBeEmpty() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new KinesisFirehoseSink<>(
                                        elementConverter,
                                        500,
                                        16,
                                        10000,
                                        4 * 1024 * 1024L,
                                        5000L,
                                        1000 * 1024L,
                                        false,
                                        "",
                                        new Properties()))
                .withMessageContaining(
                        "The delivery stream name must be set when initializing the KDF Sink.");
    }

    @Test
    public void firehoseSinkFailsWhenAccessKeyIdIsNotProvided() {
        Properties properties = createConfig("https://non-exisitent-location");
        properties.setProperty(
                AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.BASIC.toString());
        properties.remove(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER));
        firehoseSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
                properties, "Please set values for AWS Access Key ID");
    }

    @Test
    public void firehoseSinkFailsWhenRegionIsNotProvided() {
        Properties properties = createConfig("https://non-exisitent-location");
        properties.remove(AWS_REGION);
        firehoseSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
                properties, "region must not be null.");
    }

    @Test
    public void firehoseSinkFailsWhenUnableToConnectToRemoteService() {
        Properties properties = createConfig("https://non-exisitent-location");
        properties.remove(TRUST_ALL_CERTIFICATES);
        firehoseSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
                properties,
                "Received an UnknownHostException when attempting to interact with a service.");
    }

    private void firehoseSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
            Properties properties, String errorMessage) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KinesisFirehoseSink<String> kdsSink =
                KinesisFirehoseSink.<String>builder()
                        .setSerializationSchema(new SimpleStringSchema())
                        .setDeliveryStreamName("non-existent-stream")
                        .setMaxBatchSize(1)
                        .setFirehoseClientProperties(properties)
                        .build();

        KinesisFirehoseTestUtils.getSampleDataGenerator(env, 10).sinkTo(kdsSink);

        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(() -> env.execute("Integration Test"))
                .havingCause()
                .havingCause()
                .withMessageContaining(errorMessage);
    }
}
