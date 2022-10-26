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

package org.apache.flink.connector.firehose.sink.testutils;

import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamType;
import software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration;
import software.amazon.awssdk.utils.ImmutableMap;

import java.util.ArrayList;
import java.util.List;

/**
 * A set of static methods that can be used to call common AWS services on the Localstack container.
 */
public class KinesisFirehoseTestUtils {

    private static final ObjectMapper MAPPER = JacksonMapperFactory.createObjectMapper();

    public static FirehoseClient createFirehoseClient(String endpoint, SdkHttpClient httpClient) {
        return AWSServicesTestUtils.createAwsSyncClient(
                endpoint, httpClient, FirehoseClient.builder());
    }

    public static void createDeliveryStream(
            String deliveryStreamName,
            String bucketName,
            String roleARN,
            FirehoseClient firehoseClient) {
        ExtendedS3DestinationConfiguration s3Config =
                ExtendedS3DestinationConfiguration.builder()
                        .bucketARN(bucketName)
                        .roleARN(roleARN)
                        .build();
        CreateDeliveryStreamRequest request =
                CreateDeliveryStreamRequest.builder()
                        .deliveryStreamName(deliveryStreamName)
                        .extendedS3DestinationConfiguration(s3Config)
                        .deliveryStreamType(DeliveryStreamType.DIRECT_PUT)
                        .build();

        firehoseClient.createDeliveryStream(request);
    }

    public static DataStream<String> getSampleDataGenerator(
            StreamExecutionEnvironment env, int endValue) {
        return env.fromSequence(1, endValue)
                .map(Object::toString)
                .returns(String.class)
                .map(data -> MAPPER.writeValueAsString(ImmutableMap.of("data", data)));
    }

    public static List<String> getSampleData(int endValue) throws JsonProcessingException {
        List<String> expectedElements = new ArrayList<>();
        for (int i = 1; i <= endValue; i++) {
            expectedElements.add(
                    MAPPER.writeValueAsString(ImmutableMap.of("data", String.valueOf(i))));
        }
        return expectedElements;
    }
}
