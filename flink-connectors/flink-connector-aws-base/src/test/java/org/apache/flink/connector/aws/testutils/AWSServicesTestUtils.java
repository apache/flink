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

package org.apache.flink.connector.aws.testutils;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

/**
 * A set of static methods that can be used to call common AWS services on the Localstack container.
 */
public class AWSServicesTestUtils {

    private static final String ACCESS_KEY_ID = "accessKeyId";
    private static final String SECRET_ACCESS_KEY = "secretAccessKey";

    public static S3Client createS3Client(String endpoint, SdkHttpClient httpClient) {
        return createAwsSyncClient(endpoint, httpClient, S3Client.builder());
    }

    public static IamClient createIamClient(String endpoint, SdkHttpClient httpClient) {
        return createAwsSyncClient(endpoint, httpClient, IamClient.builder());
    }

    public static <
                    S extends SdkClient,
                    T extends
                            AwsSyncClientBuilder<? extends T, S> & AwsClientBuilder<? extends T, S>>
            S createAwsSyncClient(String endpoint, SdkHttpClient httpClient, T clientBuilder) {
        Properties config = createConfig(endpoint);
        return clientBuilder
                .httpClient(httpClient)
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(AWSGeneralUtil.getCredentialsProvider(config))
                .region(AWSGeneralUtil.getRegion(config))
                .build();
    }

    public static Properties createConfig(String endpoint) {
        Properties config = new Properties();
        config.setProperty(AWS_REGION, Region.AP_SOUTHEAST_1.toString());
        config.setProperty(AWS_ENDPOINT, endpoint);
        config.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), ACCESS_KEY_ID);
        config.setProperty(
                AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), SECRET_ACCESS_KEY);
        config.setProperty(TRUST_ALL_CERTIFICATES, "true");
        return config;
    }

    public static SdkHttpClient createHttpClient() {
        AttributeMap.Builder attributeMapBuilder = AttributeMap.builder();
        attributeMapBuilder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true);
        attributeMapBuilder.put(SdkHttpConfigurationOption.PROTOCOL, Protocol.HTTP1_1);
        return ApacheHttpClient.builder().buildWithDefaults(attributeMapBuilder.build());
    }

    public static void createBucket(S3Client s3Client, String bucketName) {
        CreateBucketRequest bucketRequest =
                CreateBucketRequest.builder().bucket(bucketName).build();
        s3Client.createBucket(bucketRequest);

        HeadBucketRequest bucketRequestWait =
                HeadBucketRequest.builder().bucket(bucketName).build();

        try (final S3Waiter waiter = s3Client.waiter()) {
            waiter.waitUntilBucketExists(bucketRequestWait);
        }
    }

    public static void createIAMRole(IamClient iam, String roleName) {
        CreateRoleRequest request = CreateRoleRequest.builder().roleName(roleName).build();

        iam.createRole(request);
    }

    public static List<S3Object> listBucketObjects(S3Client s3, String bucketName) {
        ListObjectsV2Request listObjects =
                ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Response res = s3.listObjectsV2(listObjects);
        return res.contents();
    }

    public static <T> List<T> readObjectsFromS3Bucket(
            S3Client s3Client,
            List<S3Object> objects,
            String bucketName,
            Function<ResponseBytes<GetObjectResponse>, T> deserializer) {
        return objects.stream()
                .map(
                        object ->
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(object.key())
                                        .build())
                .map(s3Client::getObjectAsBytes)
                .map(deserializer)
                .collect(Collectors.toList());
    }
}
