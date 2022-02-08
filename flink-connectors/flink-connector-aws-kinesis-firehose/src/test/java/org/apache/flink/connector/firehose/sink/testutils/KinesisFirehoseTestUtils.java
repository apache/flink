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

import org.apache.flink.connector.aws.util.AWSAsyncSinkUtil;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseConfigConstants;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamType;
import software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;

/**
 * A set of static methods that can be used to call common AWS services on the Localstack container.
 */
public class KinesisFirehoseTestUtils {

    public static FirehoseAsyncClient getFirehoseClient(
            String endpoint, SdkAsyncHttpClient httpClient) throws URISyntaxException {
        return AWSAsyncSinkUtil.createAwsAsyncClient(
                createConfig(endpoint),
                httpClient,
                FirehoseAsyncClient.builder()
                        .httpClient(httpClient)
                        .endpointOverride(new URI(endpoint)),
                KinesisFirehoseConfigConstants.BASE_FIREHOSE_USER_AGENT_PREFIX_FORMAT,
                KinesisFirehoseConfigConstants.FIREHOSE_CLIENT_USER_AGENT_PREFIX);
    }

    public static void createDeliveryStream(
            String deliveryStreamName,
            String bucketName,
            String roleARN,
            FirehoseAsyncClient firehoseAsyncClient)
            throws ExecutionException, InterruptedException {
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

        CompletableFuture<CreateDeliveryStreamResponse> deliveryStream =
                firehoseAsyncClient.createDeliveryStream(request);
        deliveryStream.get();
    }
}
