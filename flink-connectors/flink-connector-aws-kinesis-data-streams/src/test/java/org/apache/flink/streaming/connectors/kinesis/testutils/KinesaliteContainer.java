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

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;

/**
 * A testcontainer based on Kinesalite.
 *
 * <p>Note that the more obvious localstack container with Kinesis took 1 minute to start vs 10
 * seconds of Kinesalite.
 */
public class KinesaliteContainer extends GenericContainer<KinesaliteContainer> {
    private static final String ACCESS_KEY = "access key";
    private static final String SECRET_KEY = "secret key";
    private static final Region REGION = Region.US_EAST_1;

    public KinesaliteContainer(DockerImageName imageName) {
        super(imageName);

        withEnv(ACCESS_KEY_ENV_VAR, ACCESS_KEY);
        withEnv(SECRET_KEY_ENV_VAR, ACCESS_KEY);
        withExposedPorts(4567);
        waitingFor(new ListStreamsWaitStrategy());
        tryStartContainer();
    }

    /** Returns the endpoint url to access the container from outside the docker network. */
    public String getContainerEndpointUrl() {
        return String.format("https://%s:%s", getContainerIpAddress(), getMappedPort(4567));
    }

    /** Returns the endpoint url to access the host from inside the docker network. */
    public String getHostEndpointUrl() {
        return String.format("https://%s:%s", getHost(), getMappedPort(4567));
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
    public AmazonKinesis getContainerClient() {
        return getClient(getContainerEndpointUrl());
    }

    /** Returns the client to access the host from inside the docker network. */
    public AmazonKinesis getHostClient() {
        return getClient(getHostEndpointUrl());
    }

    private AmazonKinesis getClient(String endPoint) {
        return AmazonKinesisClientBuilder.standard()
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(getAccessKey(), getSecretKey())))
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endPoint, "us-east-1"))
                .build();
    }

    public KinesisAsyncClient getV2Client() throws URISyntaxException {
        return KinesisAsyncClient.builder()
                .endpointOverride(new URI(getHostEndpointUrl()))
                .region(REGION)
                .credentialsProvider(
                        () -> AwsBasicCredentials.create(getAccessKey(), getSecretKey()))
                .httpClient(buildSdkAsyncHttpClient())
                .build();
    }

    private void tryStartContainer() {
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
            Unreliables.retryUntilSuccess(
                    (int) this.startupTimeout.getSeconds() * TIMEOUT_PER_RETRY,
                    TimeUnit.SECONDS,
                    () -> rateLimiter.getWhenReady(() -> tryList()));
        }

        private ListStreamsResult tryList() {
            tryStartContainer();
            return getContainerClient().listStreams();
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
