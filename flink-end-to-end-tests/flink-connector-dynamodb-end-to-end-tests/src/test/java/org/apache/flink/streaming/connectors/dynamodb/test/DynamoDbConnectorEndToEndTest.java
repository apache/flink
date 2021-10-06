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

package org.apache.flink.streaming.connectors.dynamodb.test;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbProducer;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbSink;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbSinkFunction;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteResponse;
import org.apache.flink.streaming.connectors.dynamodb.WriteRequestFailureHandler;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** End to End Tests for DynamoDb connector. */
public class DynamoDbConnectorEndToEndTest {

    private static final String TEST_TABLE = "test_table";
    private static final String KEY_ATTRIBUTE = "key_attribute";

    private static DynamoDbClient dynamoDbClient;

    @ClassRule
    public static GenericContainer<?> dynamoDBLocal =
            new GenericContainer<>("amazon/dynamodb-local:1.16.0").withExposedPorts(8000);

    @BeforeClass
    public static void setUp() {
        dynamoDbClient =
                DynamoDbClient.builder()
                        .endpointOverride(URI.create(getDynamoDbUrl()))
                        .region(Region.US_EAST_1)
                        .credentialsProvider(() -> AwsBasicCredentials.create("x", "y"))
                        .build();
        dynamoDbClient.createTable(
                CreateTableRequest.builder()
                        .tableName(TEST_TABLE)
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName(KEY_ATTRIBUTE)
                                        .attributeType(ScalarAttributeType.S)
                                        .build())
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName(KEY_ATTRIBUTE)
                                        .keyType(KeyType.HASH)
                                        .build())
                        .provisionedThroughput(
                                ProvisionedThroughput.builder()
                                        .readCapacityUnits(10L)
                                        .writeCapacityUnits(10L)
                                        .build())
                        .build());
    }

    @AfterClass
    public static void tearDown() {
        dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(TEST_TABLE).build());
        dynamoDbClient.close();
    }

    private Properties getDynamoDBProperties() {
        Properties properties = new Properties();
        properties.put("aws.region", "us-east-1");
        properties.put("aws.endpoint", getDynamoDbUrl());
        properties.put("aws.credentials.provider.basic.accesskeyid", "x");
        properties.put("aws.credentials.provider.basic.secretkey", "y");
        return properties;
    }

    @Test
    public void test() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(3);

        DynamoDbTablesConfig tablesConfig = new DynamoDbTablesConfig();
        tablesConfig.addTableConfig(TEST_TABLE, KEY_ATTRIBUTE);

        DynamoDbSink<String> dynamoDbSink =
                new DynamoDbSink<>(new DynamoDBTestSinkFunction(), getDynamoDBProperties());
        dynamoDbSink.setFailOnError(true);
        dynamoDbSink.setBatchSize(4);
        dynamoDbSink.setDynamoDbTablesConfig(tablesConfig);

        List<String> input =
                ImmutableList.of(
                        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
                        "Ten");

        env.fromCollection(input).addSink(dynamoDbSink);
        env.execute("DynamoDB End to End Test");

        List<ImmutableMap<String, AttributeValue>> keys =
                input.stream()
                        .map(
                                k ->
                                        ImmutableMap.of(
                                                KEY_ATTRIBUTE,
                                                AttributeValue.builder().s(k).build()))
                        .collect(Collectors.toList());

        BatchGetItemResponse batchGetItemResponse =
                dynamoDbClient.batchGetItem(
                        BatchGetItemRequest.builder()
                                .requestItems(
                                        ImmutableMap.of(
                                                TEST_TABLE,
                                                KeysAndAttributes.builder().keys(keys).build()))
                                .build());
        Map<String, List<Map<String, AttributeValue>>> responses = batchGetItemResponse.responses();
        List<Map<String, AttributeValue>> insertedKeys = responses.get(TEST_TABLE);
        assertNotNull(insertedKeys);
        assertEquals(10, insertedKeys.size());

        Set<String> inserted =
                insertedKeys.stream()
                        .map(m -> m.get(KEY_ATTRIBUTE))
                        .map(AttributeValue::s)
                        .collect(Collectors.toSet());
        for (String key : input) {
            assertTrue("Missing " + key, inserted.contains(key));
        }
    }

    @Test(expected = Exception.class)
    public void testSinkThrowsExceptionOnFailure() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties properties = new Properties();
        properties.put("aws.region", "us-east-1");
        properties.put("aws.endpoint", "http://unknown-host");
        properties.put("aws.credentials.provider.basic.accesskeyid", "x");
        properties.put("aws.credentials.provider.basic.secretkey", "y");

        DynamoDbSink<String> dynamoDbSink =
                new DynamoDbSink<>(new DynamoDBTestSinkFunction(), properties);
        dynamoDbSink.setFailOnError(true);
        dynamoDbSink.setBatchSize(1);
        dynamoDbSink.setDynamoDbTablesConfig(new DynamoDbTablesConfig());

        env.fromCollection(ImmutableList.of("one")).addSink(dynamoDbSink);
        env.execute("DynamoDB End to End Test with Exception");
    }

    @Test
    public void testSendsFailedRecordToWriteRequestFailureHandler() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        List<String> messages = ImmutableList.of("Locaste", "Isonoe", "Kore", "Arche");

        DynamoDbSinkFunction<String> failingSinkFunction =
                (value, context, dynamoDbProducer) ->
                        dynamoDbProducer.produce(
                                PutItemRequest.builder()
                                        .tableName("missing_table") // will throw resource not found
                                        .item(
                                                ImmutableMap.of(
                                                        KEY_ATTRIBUTE,
                                                        AttributeValue.builder().s(value).build()))
                                        .build());

        DynamoDbSink<String> dynamoDbSink =
                new DynamoDbSink<>(
                        failingSinkFunction,
                        getDynamoDBProperties(),
                        new AssertingFailureHandler(messages.size()));
        dynamoDbSink.setFailOnError(true);
        dynamoDbSink.setBatchSize(25);
        dynamoDbSink.setDynamoDbTablesConfig(new DynamoDbTablesConfig());

        env.fromCollection(messages).addSink(dynamoDbSink);
        env.execute("DynamoDB End to End Test with missing table");
    }

    private static String getDynamoDbUrl() {
        return String.format(
                "http://%s:%s", dynamoDBLocal.getHost(), dynamoDBLocal.getFirstMappedPort());
    }

    private static final class AssertingFailureHandler implements WriteRequestFailureHandler {

        private final int expectedMessagesSize;

        public AssertingFailureHandler(int expectedMessagesSize) {
            this.expectedMessagesSize = expectedMessagesSize;
        }

        @Override
        public void onFailure(ProducerWriteRequest request, Throwable failure) throws Throwable {
            assertEquals(
                    "Number of messages in the failed request",
                    expectedMessagesSize, // expect all messages in one write request, as
                    // batch is bigger than number of messages
                    request.getRequests().size());
            assertTrue(failure instanceof ResourceNotFoundException);
        }

        @Override
        public void onFailure(ProducerWriteRequest request, ProducerWriteResponse response)
                throws Throwable {}
    }

    private static final class DynamoDBTestSinkFunction implements DynamoDbSinkFunction<String> {
        private static final long serialVersionUID = -6878686637563351934L;

        @Override
        public void process(
                String value, RuntimeContext context, DynamoDbProducer dynamoDbProducer) {
            dynamoDbProducer.produce(
                    PutItemRequest.builder()
                            .tableName(TEST_TABLE)
                            .item(
                                    ImmutableMap.of(
                                            KEY_ATTRIBUTE,
                                            AttributeValue.builder().s(value).build()))
                            .build());
        }
    }
}
