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
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.DockerImageVersions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createBucket;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIAMRole;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIamClient;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createS3Client;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.listBucketObjects;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.readObjectsFromS3Bucket;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createDeliveryStream;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createFirehoseClient;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test suite for the {@code KinesisFirehoseSink} using a localstack container. */
@Testcontainers
class KinesisFirehoseSinkITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisFirehoseSinkITCase.class);
    private static final String ROLE_NAME = "super-role";
    private static final String ROLE_ARN = "arn:aws:iam::000000000000:role/" + ROLE_NAME;
    private static final String BUCKET_NAME = "s3-firehose";
    private static final String STREAM_NAME = "s3-stream";
    private static final int NUMBER_OF_ELEMENTS = 92;
    private StreamExecutionEnvironment env;

    private SdkHttpClient httpClient;
    private S3Client s3Client;
    private FirehoseClient firehoseClient;
    private IamClient iamClient;

    @Container
    private static LocalstackContainer mockFirehoseContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK));

    @BeforeEach
    void setup() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        httpClient = AWSServicesTestUtils.createHttpClient();
        s3Client = createS3Client(mockFirehoseContainer.getEndpoint(), httpClient);
        firehoseClient = createFirehoseClient(mockFirehoseContainer.getEndpoint(), httpClient);
        iamClient = createIamClient(mockFirehoseContainer.getEndpoint(), httpClient);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @AfterEach
    void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    void firehoseSinkWritesCorrectDataToMockAWSServices() throws Exception {
        LOG.info("1 - Creating the bucket for Firehose to deliver into...");
        createBucket(s3Client, BUCKET_NAME);
        LOG.info("2 - Creating the IAM Role for Firehose to write into the s3 bucket...");
        createIAMRole(iamClient, ROLE_NAME);
        LOG.info("3 - Creating the Firehose delivery stream...");
        createDeliveryStream(STREAM_NAME, BUCKET_NAME, ROLE_ARN, firehoseClient);

        KinesisFirehoseSink<String> kdsSink =
                KinesisFirehoseSink.<String>builder()
                        .setSerializationSchema(new SimpleStringSchema())
                        .setDeliveryStreamName(STREAM_NAME)
                        .setMaxBatchSize(1)
                        .setFirehoseClientProperties(
                                createConfig(mockFirehoseContainer.getEndpoint()))
                        .build();

        KinesisFirehoseTestUtils.getSampleDataGenerator(env, NUMBER_OF_ELEMENTS).sinkTo(kdsSink);
        env.execute("Integration Test");

        List<S3Object> objects =
                listBucketObjects(
                        createS3Client(mockFirehoseContainer.getEndpoint(), httpClient),
                        BUCKET_NAME);
        assertThat(objects.size()).isEqualTo(NUMBER_OF_ELEMENTS);
        assertThat(
                        readObjectsFromS3Bucket(
                                s3Client,
                                objects,
                                BUCKET_NAME,
                                response -> new String(response.asByteArrayUnsafe())))
                .containsAll(KinesisFirehoseTestUtils.getSampleData(NUMBER_OF_ELEMENTS));
    }
}
