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

package org.apache.flink.streaming.kinesis.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisPubsubClient;
import org.apache.flink.streaming.kinesis.test.containers.KinesaliteContainer;
import org.apache.flink.streaming.kinesis.test.model.Order;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.FlinkContainer;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** End-to-end test for Kinesis Table API using Kinesalite. */
@Category(value = {TravisGroup1.class})
public class KinesisTableApiITCase extends TestLogger {
    private static final String ORDERS_STREAM = "orders";
    private static final String LARGE_ORDERS_STREAM = "large_orders";

    private final Path sqlConnectorKinesisJar = TestUtils.getResource(".*kinesis.jar");
    private final Network network = Network.newNetwork();

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @Rule
    public final KinesaliteContainer kinesalite = new KinesaliteContainer().withNetwork(network);

    private KinesisPubsubClient kinesisClient;

    @Rule
    public final FlinkContainer flink =
            FlinkContainer.builder()
                    .build()
                    .withEnv("AWS_ACCESS_KEY_ID", "fakeid")
                    .withEnv("AWS_SECRET_KEY", "fakekey")
                    .withEnv("AWS_CBOR_DISABLE", "1")
                    .withEnv(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.kinesis.shaded.com.amazonaws.sdk.disableCertChecking")
                    .withNetwork(network)
                    .dependsOn(kinesalite);

    @Before
    public void setUp() throws Exception {
        // Required for Kinesalite.
        // Including shaded and non-shaded conf to support test running from Maven and IntelliJ
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "1");
        System.setProperty("com.amazonaws.sdk.disableCbor", "1");
        System.setProperty(
                "org.apache.flink.kinesis.shaded.com.amazonaws.sdk.disableCertChecking", "1");
        System.setProperty("org.apache.flink.kinesis.shaded.com.amazonaws.sdk.disableCbor", "1");

        Properties properties = new Properties();
        properties.setProperty(AWS_ENDPOINT, kinesalite.getEndpointUrl());
        properties.setProperty(AWS_ACCESS_KEY_ID, "ak");
        properties.setProperty(AWS_SECRET_ACCESS_KEY, "sk");

        kinesisClient = new KinesisPubsubClient(properties);
        kinesisClient.createTopic(ORDERS_STREAM, 1, properties);
        kinesisClient.createTopic(LARGE_ORDERS_STREAM, 1, properties);
    }

    @Test(timeout = 120_000)
    public void testTableApiSourceAndSink() throws Exception {
        List<Order> smallOrders = ImmutableList.of(new Order("A", 5), new Order("B", 10));

        // filter-large-orders.sql is supposed to preserve orders with quantity > 10
        List<Order> expected =
                ImmutableList.of(new Order("C", 15), new Order("D", 20), new Order("E", 25));

        smallOrders.forEach(order -> kinesisClient.sendMessage(ORDERS_STREAM, toJson(order)));
        expected.forEach(order -> kinesisClient.sendMessage(ORDERS_STREAM, toJson(order)));

        executeSqlStatements(readSqlFile("filter-large-orders.sql"));

        List<Order> result = readAllOrdersFromKinesis(kinesisClient);
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    private List<Order> readAllOrdersFromKinesis(final KinesisPubsubClient client)
            throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(5));
        List<Order> orders;
        do {
            Thread.sleep(1000);
            orders =
                    client.readAllMessages(LARGE_ORDERS_STREAM).stream()
                            .map(order -> fromJson(order, Order.class))
                            .collect(Collectors.toList());
        } while (deadline.hasTimeLeft() && orders.size() < 3);

        return orders;
    }

    private List<String> readSqlFile(final String resourceName) throws Exception {
        return Files.readAllLines(Paths.get(getClass().getResource(resourceName).toURI()));
    }

    private void executeSqlStatements(final List<String> sqlLines) throws Exception {
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorKinesisJar)
                        .build());
    }

    private <T> String toJson(final T object) {
        try {
            return new ObjectMapper().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Test Failure.", e);
        }
    }

    private <T> T fromJson(final String json, final Class<T> type) {
        try {
            return new ObjectMapper().readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Test Failure.", e);
        }
    }
}
