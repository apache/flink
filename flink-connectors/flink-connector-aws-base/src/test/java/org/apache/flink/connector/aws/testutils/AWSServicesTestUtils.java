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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.waiters.S3AsyncWaiter;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

    public static S3AsyncClient createS3Client(String endpoint, SdkAsyncHttpClient httpClient) {
        return S3AsyncClient.builder()
                .httpClient(httpClient)
                .region(Region.AP_SOUTHEAST_1)
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(createDefaultCredentials())
                .build();
    }

    public static IamAsyncClient createIamClient(String endpoint, SdkAsyncHttpClient httpClient) {
        return IamAsyncClient.builder()
                .httpClient(httpClient)
                .region(Region.AWS_GLOBAL)
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(createDefaultCredentials())
                .build();
    }

    public static AwsCredentialsProvider createDefaultCredentials() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY));
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

    public static SdkAsyncHttpClient createHttpClient(String endpoint) {
        return AWSGeneralUtil.createAsyncHttpClient(
                createConfig(endpoint),
                NettyNioAsyncHttpClient.builder()
                        .eventLoopGroupBuilder(SdkEventLoopGroup.builder()));
    }

    public static void createBucket(S3AsyncClient s3Client, String bucketName)
            throws ExecutionException, InterruptedException {
        CreateBucketRequest bucketRequest =
                CreateBucketRequest.builder().bucket(bucketName).build();
        s3Client.createBucket(bucketRequest);

        HeadBucketRequest bucketRequestWait =
                HeadBucketRequest.builder().bucket(bucketName).build();

        try (final S3AsyncWaiter waiter = s3Client.waiter()) {
            waiter.waitUntilBucketExists(bucketRequestWait).get();
        }
    }

    public static void createIAMRole(IamAsyncClient iam, String roleName)
            throws ExecutionException, InterruptedException {
        CreateRoleRequest request = CreateRoleRequest.builder().roleName(roleName).build();

        CompletableFuture<CreateRoleResponse> responseFuture = iam.createRole(request);
        responseFuture.get();
    }

    public static List<S3Object> listBucketObjects(S3AsyncClient s3, String bucketName)
            throws ExecutionException, InterruptedException {
        ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
        CompletableFuture<ListObjectsResponse> res = s3.listObjects(listObjects);
        return res.get().contents();
    }

    public static <T> List<T> readObjectsFromS3Bucket(
            S3AsyncClient s3AsyncClient,
            List<S3Object> objects,
            String bucketName,
            Function<ResponseBytes<GetObjectResponse>, T> deserializer) {
        S3BucketReader bucketReader = new S3BucketReader(s3AsyncClient, bucketName);
        return bucketReader.readObjects(objects, deserializer);
    }

    /** Helper class to read objects from S3. */
    private static class S3BucketReader {
        private final S3AsyncClient s3AsyncClient;
        private final String bucketName;

        public S3BucketReader(S3AsyncClient s3AsyncClient, String bucketName) {
            this.s3AsyncClient = s3AsyncClient;
            this.bucketName = bucketName;
        }

        public <T> List<T> readObjects(
                List<S3Object> objectList,
                Function<ResponseBytes<GetObjectResponse>, T> deserializer) {
            return objectList.stream()
                    .map(object -> readObjectWitKey(object.key(), deserializer))
                    .collect(Collectors.toList());
        }

        public <T> T readObjectWitKey(
                String key, Function<ResponseBytes<GetObjectResponse>, T> deserializer) {
            GetObjectRequest getObjectRequest =
                    GetObjectRequest.builder().bucket(bucketName).key(key).build();
            return s3AsyncClient
                    .getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
                    .thenApply(deserializer)
                    .join();
        }
    }
}
