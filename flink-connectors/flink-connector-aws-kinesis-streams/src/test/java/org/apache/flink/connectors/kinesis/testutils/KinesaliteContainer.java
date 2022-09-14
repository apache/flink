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

package org.apache.flink.connectors.kinesis.testutils;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;

import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;

import java.util.Properties;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.SECONDS;

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

    public KinesaliteContainer(DockerImageName imageName) {
        super(imageName);

        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

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
    public KinesisClient createContainerClient(SdkHttpClient httpClient) {
        return AWSServicesTestUtils.createAwsSyncClient(
                getContainerEndpointUrl(), httpClient, KinesisClient.builder());
    }

    /** Returns the client to access the host from inside the docker network. */
    public KinesisClient createHostClient(SdkHttpClient httpClient) {
        return AWSServicesTestUtils.createAwsSyncClient(
                getHostEndpointUrl(), httpClient, KinesisClient.builder());
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
        private static final int TRANSACTIONS_PER_SECOND = 1;

        private final RateLimiter rateLimiter =
                RateLimiterBuilder.newBuilder()
                        .withRate(TRANSACTIONS_PER_SECOND, SECONDS)
                        .withConstantThroughput()
                        .build();

        @Override
        protected void waitUntilReady() {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new IllegalStateException("Kinesalite Container startup was interrupted");
            }

            retryUntilSuccessRunner(this::list);
        }

        protected <T> void retryUntilSuccessRunner(final Callable<T> lambda) {
            Unreliables.retryUntilSuccess(
                    (int) startupTimeout.getSeconds(),
                    SECONDS,
                    () -> rateLimiter.getWhenReady(lambda));
        }

        private ListStreamsResponse list() {
            try (SdkHttpClient httpClient = AWSServicesTestUtils.createHttpClient();
                    KinesisClient containerClient = createContainerClient(httpClient)) {
                return containerClient.listStreams();
            }
        }
    }
}
