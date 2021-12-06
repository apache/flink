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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A testcontainer based on Kinesalite.
 *
 * <p>Note that the more obvious localstack container with Kinesis took 1 minute to start vs 10
 * seconds of Kinesalite.
 */
public class KinesaliteContainer extends GenericContainer<KinesaliteContainer> {
    private static final String ACCESS_KEY = "access key";
    private static final String SECRET_KEY = "secret key";
    private static final int PORT = 4567;
    private static final Region REGION = Region.US_EAST_1;
    private static final String URL_FORMAT = "https://%s:%s";

    private static final Logger LOG = LoggerFactory.getLogger(KinesaliteContainer.class);

    public KinesaliteContainer(DockerImageName imageName) {
        super(imageName);

        withExposedPorts(PORT);
        waitingFor(new ListStreamsWaitStrategy());
        startContainer();
    }

    /** Returns the endpoint url to access the container from outside the docker network. */
    public String getContainerEndpointUrl() {
        return String.format(URL_FORMAT, getContainerIpAddress(), getMappedPort(PORT));
    }

    /** Returns the endpoint url to access the host from inside the docker network. */
    public String getHostEndpointUrl() {
        return String.format(URL_FORMAT, getHost(), getMappedPort(PORT));
    }

    public String getAccessKey() {
        return ACCESS_KEY;
    }

    public String getSecretKey() {
        return SECRET_KEY;
    }

    public Region getRegion() {
        return REGION;
    }

    /** Returns the properties to access the container from outside the docker network. */
    public Properties getContainerProperties() {
        return getProperties(getContainerEndpointUrl());
    }

    /** Returns the properties to access the host from inside the docker network. */
    public Properties getHostProperties() {
        return getProperties(getHostEndpointUrl());
    }

    /** Returns the client to access the container from outside the docker network. */
    public KinesisAsyncClient getContainerClient() throws URISyntaxException {
        return getClient(getContainerEndpointUrl());
    }

    /** Returns the client to access the host from inside the docker network. */
    public KinesisAsyncClient getHostClient() throws URISyntaxException {
        return getClient(getHostEndpointUrl());
    }

    public KinesisAsyncClient getClient(String endPoint) throws URISyntaxException {
        return KinesisAsyncClient.builder()
                .endpointOverride(new URI(endPoint))
                .region(REGION)
                .credentialsProvider(
                        () -> AwsBasicCredentials.create(getAccessKey(), getSecretKey()))
                .httpClient(buildSdkAsyncHttpClient())
                .build();
    }

    public void prepareStream(String streamName)
            throws ExecutionException, InterruptedException, URISyntaxException {
        KinesisAsyncClient kinesisClient = getHostClient();
        kinesisClient
                .createStream(
                        CreateStreamRequest.builder().streamName(streamName).shardCount(1).build())
                .get();
        waitingFor(new ActiveStreamWaitStrategy(kinesisClient, streamName));
    }

    private void startContainer() {
        withCreateContainerCmdModifier(
                cmd ->
                        cmd.withEntrypoint(
                                "/tini",
                                "--",
                                "/usr/src/app/node_modules/kinesalite/cli.js",
                                "--path",
                                "/var/lib/kinesalite",
                                "--ssl"));
    }

    private Properties getProperties(String endpointUrl) {
        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_REGION, REGION.toString());
        config.setProperty(AWSConfigConstants.AWS_ENDPOINT, endpointUrl);
        config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, getAccessKey());
        config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, getSecretKey());
        return config;
    }

    private class ListStreamsWaitStrategy extends AbstractWaitStrategy {
        private static final int TIMEOUT_PER_RETRY = 15;

        private final RateLimiter rateLimiter =
                RateLimiterBuilder.newBuilder()
                        .withRate(TIMEOUT_PER_RETRY, TimeUnit.SECONDS)
                        .withConstantThroughput()
                        .build();

        @Override
        protected void waitUntilReady() {
            retryUntilSuccessRunner(() -> list());
        }

        protected <T> void retryUntilSuccessRunner(final Callable<T> lambda) {
            Unreliables.retryUntilSuccess(
                    (int) this.startupTimeout.getSeconds() * TIMEOUT_PER_RETRY,
                    TimeUnit.SECONDS,
                    () -> rateLimiter.getWhenReady(() -> lambda.call()));
        }

        private ListStreamsResponse list()
                throws ExecutionException, InterruptedException, URISyntaxException {
            startContainer();
            LOG.trace(" *** LOGGING CONTAINER OUTPUT TO TRACE SLFJ *** ");
            LOG.trace(getLogs());
            System.out.println(" *** LOGGING CONTAINER OUTPUT TO S.OUT *** ");
            System.out.println(getLogs());
            System.err.println(" *** LOGGING CONTAINER OUTPUT TO S.ERR *** ");
            System.err.println(getLogs());
            return getContainerClient().listStreams().get();
        }
    }

    private class ActiveStreamWaitStrategy extends ListStreamsWaitStrategy {
        private KinesisAsyncClient kinesisClient;
        private String streamName;

        protected ActiveStreamWaitStrategy(KinesisAsyncClient kinesisClient, String streamName) {
            this.kinesisClient = kinesisClient;
            this.streamName = streamName;
        }

        @Override
        protected void waitUntilReady() {
            retryUntilSuccessRunner(() -> waitForStream());
        }

        private DescribeStreamResponse waitForStream()
                throws ExecutionException, InterruptedException {
            DescribeStreamResponse describeStream;
            do {
                describeStream =
                        kinesisClient
                                .describeStream(
                                        DescribeStreamRequest.builder()
                                                .streamName(streamName)
                                                .build())
                                .get();
            } while (describeStream.streamDescription().streamStatus() != StreamStatus.ACTIVE);
            return describeStream;
        }
    }

    private SdkAsyncHttpClient buildSdkAsyncHttpClient() {
        return NettyNioAsyncHttpClient.builder()
                .buildWithDefaults(
                        AttributeMap.builder()
                                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                                .build());
    }
}
