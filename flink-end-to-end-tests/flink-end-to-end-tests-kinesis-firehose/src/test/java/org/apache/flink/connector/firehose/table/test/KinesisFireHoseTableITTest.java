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
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.tests.util.flink.container.FlinkContainers;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createBucket;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createHttpClient;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIAMRole;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIamClient;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createS3Client;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.listBucketObjects;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createDeliveryStream;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.getFirehoseClient;

/** End to End test for Kinesis Firehose Table sink API. */
public class KinesisFireHoseTableITTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisFireHoseTableITTest.class);

    private static final String ROLE_NAME = "super-role";
    private static final String ROLE_ARN = "arn:aws:iam::000000000000:role/" + ROLE_NAME;
    private static final String BUCKET_NAME = "s3-firehose";
    private static final String STREAM_NAME = "s3-stream";

    private final Path sqlConnectorFirehoseJar = TestUtils.getResource(".*firehose.jar");

    private S3AsyncClient s3AsyncClient;
    private FirehoseAsyncClient firehoseAsyncClient;
    private IamAsyncClient iamAsyncClient;

    private static final Network network = Network.newNetwork();

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @ClassRule
    public static LocalstackContainer mockFirehoseContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK))
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    public static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .setEnvironmentVariable("AWS_CBOR_DISABLE", "1")
                    .setEnvironmentVariable(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.kinesis-firehose.shaded.com.amazonaws.sdk.disableCertChecking -Daws.cborEnabled=false")
                    .setNetwork(network)
                    .setLogger(LOG)
                    .dependsOn(mockFirehoseContainer)
                    .build();

    @Before
    public void setup() throws Exception {
        SdkAsyncHttpClient httpClient = createHttpClient(mockFirehoseContainer.getEndpoint());
        s3AsyncClient = createS3Client(mockFirehoseContainer.getEndpoint(), httpClient);
        firehoseAsyncClient = getFirehoseClient(mockFirehoseContainer.getEndpoint(), httpClient);
        iamAsyncClient = createIamClient(mockFirehoseContainer.getEndpoint(), httpClient);
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        LOG.info("1 - Creating the bucket for Firehose to deliver into...");
        createBucket(s3AsyncClient, BUCKET_NAME);
        LOG.info("2 - Creating the IAM Role for Firehose to write into the s3 bucket...");
        createIAMRole(iamAsyncClient, ROLE_NAME);
        LOG.info("3 - Creating the Firehose delivery stream...");
        createDeliveryStream(STREAM_NAME, BUCKET_NAME, ROLE_ARN, firehoseAsyncClient);
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
    }

    @Test
    public void testTableApiSink() throws Exception {
        List<Order> orderList =
                ImmutableList.of(
                        new Order("A", 1),
                        new Order("B", 2),
                        new Order("C", 3),
                        new Order("D", 4),
                        new Order("E", 5));

        executeSqlStatements(readSqlFile("send-orders.sql"));
        List<Order> orders = readFromS3();
        Assertions.assertThat(orders).containsAll(orderList);
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
        List<Order> orders = new ArrayList<>();
        do {
            Thread.sleep(1000);
            ordersObjects = listBucketObjects(s3AsyncClient, BUCKET_NAME);
            ordersObjects.forEach(order -> orders.add(readFromS3(order.key())));
        } while (deadline.hasTimeLeft() && orders.size() < 5);

        return orders;
    }

    private Order readFromS3(String key) {
        GetObjectRequest getObjectRequest =
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build();
        return s3AsyncClient
                .getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
                .thenApply(
                        responseBytes ->
                                fromJson(
                                        new String(responseBytes.asByteArrayUnsafe()), Order.class))
                .join();
    }

    private <T> T fromJson(final String json, final Class<T> type) {
        try {
            return new ObjectMapper().readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Test Failure.", e);
        }
    }

    /** POJO model class for sending and receiving records on Kinesis during e2e test. */
    public static class Order {
        private final String code;
        private final int quantity;

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
