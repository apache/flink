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

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
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
public class KinesisFirehoseSinkITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisFirehoseSinkITCase.class);
    private static final String ROLE_NAME = "super-role";
    private static final String ROLE_ARN = "arn:aws:iam::000000000000:role/" + ROLE_NAME;
    private static final String BUCKET_NAME = "s3-firehose";
    private static final String STREAM_NAME = "s3-stream";
    private static final int NUMBER_OF_ELEMENTS = 92;
    private StreamExecutionEnvironment env;

    private SdkAsyncHttpClient httpClient;
    private S3AsyncClient s3AsyncClient;
    private FirehoseAsyncClient firehoseAsyncClient;
    private IamAsyncClient iamAsyncClient;

    @ClassRule
    public static LocalstackContainer mockFirehoseContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK));

    @Before
    public void setup() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        httpClient = AWSServicesTestUtils.createHttpClient(mockFirehoseContainer.getEndpoint());
        s3AsyncClient = createS3Client(mockFirehoseContainer.getEndpoint(), httpClient);
        firehoseAsyncClient = createFirehoseClient(mockFirehoseContainer.getEndpoint(), httpClient);
        iamAsyncClient = createIamClient(mockFirehoseContainer.getEndpoint(), httpClient);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    public void firehoseSinkWritesCorrectDataToMockAWSServices() throws Exception {
        LOG.info("1 - Creating the bucket for Firehose to deliver into...");
        createBucket(s3AsyncClient, BUCKET_NAME);
        LOG.info("2 - Creating the IAM Role for Firehose to write into the s3 bucket...");
        createIAMRole(iamAsyncClient, ROLE_NAME);
        LOG.info("3 - Creating the Firehose delivery stream...");
        createDeliveryStream(STREAM_NAME, BUCKET_NAME, ROLE_ARN, firehoseAsyncClient);

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
                                s3AsyncClient,
                                objects,
                                BUCKET_NAME,
                                response -> new String(response.asByteArrayUnsafe())))
                .containsAll(KinesisFirehoseTestUtils.getSampleData(NUMBER_OF_ELEMENTS));
    }
}
