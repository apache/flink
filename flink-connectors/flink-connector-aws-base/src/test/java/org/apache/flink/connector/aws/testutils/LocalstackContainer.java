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

import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A class wrapping the Localstack container that provides mock implementations of many common AWS
 * services.
 */
public class LocalstackContainer extends GenericContainer<LocalstackContainer> {

    private static final int CONTAINER_PORT = 4566;

    public LocalstackContainer(DockerImageName imageName) {
        super(imageName);
        withExposedPorts(CONTAINER_PORT);
        waitingFor(new ListBucketObjectsWaitStrategy());
    }

    public String getEndpoint() {
        return String.format("https://%s:%s", getHost(), getMappedPort(CONTAINER_PORT));
    }

    private class ListBucketObjectsWaitStrategy extends AbstractWaitStrategy {
        private static final int TRANSACTIONS_PER_SECOND = 1;

        private final RateLimiter rateLimiter =
                RateLimiterBuilder.newBuilder()
                        .withRate(TRANSACTIONS_PER_SECOND, SECONDS)
                        .withConstantThroughput()
                        .build();

        @Override
        protected void waitUntilReady() {
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new IllegalStateException("Localstack Container startup was interrupted");
            }
            Unreliables.retryUntilSuccess(
                    (int) startupTimeout.getSeconds(),
                    SECONDS,
                    () -> rateLimiter.getWhenReady(this::list));
        }

        private List<S3Object> list() throws ExecutionException, InterruptedException {
            final String bucketName = "bucket-name-not-to-be-used";
            try (final SdkAsyncHttpClient httpClient =
                            AWSServicesTestUtils.createHttpClient(getEndpoint());
                    final S3AsyncClient client =
                            AWSServicesTestUtils.createS3Client(getEndpoint(), httpClient)) {
                AWSServicesTestUtils.createBucket(client, bucketName);
                return AWSServicesTestUtils.listBucketObjects(client, bucketName);
            }
        }
    }
}
