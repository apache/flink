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

package org.apache.flink.connector.firehose.table.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createBucket;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createHttpClient;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIAMRole;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIamClient;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createS3Client;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.listBucketObjects;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.readObjectsFromS3Bucket;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createDeliveryStream;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createFirehoseClient;

/** End to End test for Kinesis Firehose Table sink API. */
public class KinesisFirehoseTableITTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisFirehoseTableITTest.class);

    private static final String ROLE_NAME = "super-role";
    private static final String ROLE_ARN = "arn:aws:iam::000000000000:role/" + ROLE_NAME;
    private static final String BUCKET_NAME = "s3-firehose";
    private static final String STREAM_NAME = "s3-stream";

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    private final Path sqlConnectorFirehoseJar = TestUtils.getResource(".*firehose.jar");

    private SdkHttpClient httpClient;
    private S3Client s3Client;
    private FirehoseClient firehoseClient;
    private IamClient iamClient;

    private static final int NUM_ELEMENTS = 5;
    private static final Network network = Network.newNetwork();

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @ClassRule
    public static LocalstackContainer mockFirehoseContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK))
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .environmentVariable("AWS_CBOR_DISABLE", "1")
                    .environmentVariable(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.kinesis-firehose.shaded.com.amazonaws.sdk.disableCertChecking -Daws.cborEnabled=false")
                    .network(network)
                    .logger(LOG)
                    .dependsOn(mockFirehoseContainer)
                    .build();

    public static final FlinkContainers FLINK =
            FlinkContainers.builder().withTestcontainersSettings(TESTCONTAINERS_SETTINGS).build();

    @Before
    public void setup() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        httpClient = createHttpClient();

        s3Client = createS3Client(mockFirehoseContainer.getEndpoint(), httpClient);
        firehoseClient = createFirehoseClient(mockFirehoseContainer.getEndpoint(), httpClient);
        iamClient = createIamClient(mockFirehoseContainer.getEndpoint(), httpClient);

        LOG.info("1 - Creating the bucket for Firehose to deliver into...");
        createBucket(s3Client, BUCKET_NAME);

        LOG.info("2 - Creating the IAM Role for Firehose to write into the s3 bucket...");
        createIAMRole(iamClient, ROLE_NAME);

        LOG.info("3 - Creating the Firehose delivery stream...");
        createDeliveryStream(STREAM_NAME, BUCKET_NAME, ROLE_ARN, firehoseClient);

        LOG.info("Done setting up the localstack.");
    }

    @BeforeClass
    public static void setupFlink() throws Exception {
        FLINK.start();
    }

    @AfterClass
    public static void stopFlink() {
        FLINK.stop();
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());

        s3Client.close();
        firehoseClient.close();
        iamClient.close();
        httpClient.close();
    }

    @Test
    public void testTableApiSink() throws Exception {
        List<Order> orderList = getTestOrders();

        executeSqlStatements(readSqlFile("send-orders.sql"));
        List<Order> orders = readFromS3();
        Assertions.assertThat(orders).containsAll(orderList);
    }

    private List<Order> getTestOrders() {
        return IntStream.range(1, NUM_ELEMENTS)
                .mapToObj(this::getOrderWithOffset)
                .collect(Collectors.toList());
    }

    private Order getOrderWithOffset(int offset) {
        return new Order(String.valueOf((char) ('A' + offset - 1)), offset);
    }

    private void executeSqlStatements(final List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorFirehoseJar)
                        .build());
    }

    private List<String> readSqlFile(final String resourceName) throws Exception {
        return Files.readAllLines(Paths.get(getClass().getResource("/" + resourceName).toURI()));
    }

    private List<Order> readFromS3() throws Exception {

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        List<S3Object> ordersObjects;
        List<Order> orders;
        do {
            Thread.sleep(1000);
            ordersObjects = listBucketObjects(s3Client, BUCKET_NAME);
            orders =
                    readObjectsFromS3Bucket(
                            s3Client,
                            ordersObjects,
                            BUCKET_NAME,
                            responseBytes ->
                                    fromJson(
                                            new String(responseBytes.asByteArrayUnsafe()),
                                            Order.class));
        } while (deadline.hasTimeLeft() && orders.size() < NUM_ELEMENTS);

        return orders;
    }

    private <T> T fromJson(final String json, final Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(String.format("Failed to deserialize json: %s", json), e);
        }
    }

    /** POJO model class for sending and receiving records on Kinesis during e2e test. */
    public static class Order {
        private final String code;
        private final int quantity;

        @JsonCreator
        public Order(
                @JsonProperty("code") final String code, @JsonProperty("quantity") int quantity) {
            this.code = code;
            this.quantity = quantity;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Order order = (Order) o;
            return quantity == order.quantity && Objects.equals(code, order.code);
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, quantity);
        }

        @Override
        public String toString() {
            return "Order{" + "code='" + code + '\'' + ", quantity=" + quantity + '}';
        }
    }
}
