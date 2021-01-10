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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_EFO_HTTP_CLIENT_MAX_CONURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.EAGER;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.NONE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;

/** Utility methods specific to Amazon Web Service SDK v2.x. */
@Internal
public class AwsV2Util {

    private static final int INITIAL_WINDOW_SIZE_BYTES = 512 * 1024; // 512 KB
    private static final Duration HEALTH_CHECK_PING_PERIOD = Duration.ofSeconds(60);
    private static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Creates an Amazon Kinesis Async Client from the provided properties. Configuration is copied
     * from AWS SDK v1 configuration class as per: -
     * https://github.com/aws/aws-sdk-java-v2/blob/2.13.52/docs/LaunchChangelog.md#134-client-override-retry-configuration
     *
     * @param configProps configuration properties
     * @param clientConfiguration the AWS SDK v1.X config ported to V2 to instantiate the client
     * @param httpClient the underlying HTTP client used to talk to Kinesis
     * @return a new Amazon Kinesis Client
     */
    public static KinesisAsyncClient createKinesisAsyncClient(
            final Properties configProps,
            final ClientConfiguration clientConfiguration,
            final SdkAsyncHttpClient httpClient) {
        final ClientOverrideConfiguration overrideConfiguration =
                createClientOverrideConfiguration(
                        clientConfiguration, ClientOverrideConfiguration.builder());
        final KinesisAsyncClientBuilder clientBuilder = KinesisAsyncClient.builder();

        return createKinesisAsyncClient(
                configProps, clientBuilder, httpClient, overrideConfiguration);
    }

    public static SdkAsyncHttpClient createHttpClient(
            final ClientConfiguration config,
            final NettyNioAsyncHttpClient.Builder httpClientBuilder,
            final Properties consumerConfig) {

        int maxConcurrency =
                Optional.ofNullable(consumerConfig.getProperty(EFO_HTTP_CLIENT_MAX_CONCURRENCY))
                        .map(Integer::parseInt)
                        .orElse(DEFAULT_EFO_HTTP_CLIENT_MAX_CONURRENCY);

        httpClientBuilder
                .maxConcurrency(maxConcurrency)
                .connectionTimeout(Duration.ofMillis(config.getConnectionTimeout()))
                .writeTimeout(Duration.ofMillis(config.getSocketTimeout()))
                .connectionMaxIdleTime(Duration.ofMillis(config.getConnectionMaxIdleMillis()))
                .useIdleConnectionReaper(config.useReaper())
                .protocol(Protocol.HTTP2)
                .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
                .http2Configuration(
                        Http2Configuration.builder()
                                .healthCheckPingPeriod(HEALTH_CHECK_PING_PERIOD)
                                .initialWindowSize(INITIAL_WINDOW_SIZE_BYTES)
                                .build());

        if (config.getConnectionTTL() > -1) {
            httpClientBuilder.connectionTimeToLive(Duration.ofMillis(config.getConnectionTTL()));
        }

        return httpClientBuilder.build();
    }

    @VisibleForTesting
    static ClientOverrideConfiguration createClientOverrideConfiguration(
            final ClientConfiguration config,
            final ClientOverrideConfiguration.Builder overrideConfigurationBuilder) {

        overrideConfigurationBuilder
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_PREFIX,
                        AWSUtil.formatFlinkUserAgentPrefix())
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_SUFFIX, config.getUserAgentSuffix());

        if (config.getRequestTimeout() > 0) {
            overrideConfigurationBuilder.apiCallAttemptTimeout(
                    Duration.ofMillis(config.getRequestTimeout()));
        }

        if (config.getClientExecutionTimeout() > 0) {
            overrideConfigurationBuilder.apiCallTimeout(
                    Duration.ofMillis(config.getClientExecutionTimeout()));
        }

        return overrideConfigurationBuilder.build();
    }

    @VisibleForTesting
    static KinesisAsyncClient createKinesisAsyncClient(
            final Properties configProps,
            final KinesisAsyncClientBuilder clientBuilder,
            final SdkAsyncHttpClient httpClient,
            final ClientOverrideConfiguration overrideConfiguration) {

        if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
            final URI endpointOverride =
                    URI.create(configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT));
            clientBuilder.endpointOverride(endpointOverride);
        }

        return clientBuilder
                .httpClient(httpClient)
                .overrideConfiguration(overrideConfiguration)
                .credentialsProvider(getCredentialsProvider(configProps))
                .region(getRegion(configProps))
                .build();
    }

    /**
     * Return a {@link AWSCredentialsProvider} instance corresponding to the configuration
     * properties.
     *
     * @param configProps the configuration properties
     * @return The corresponding AWS Credentials Provider instance
     */
    public static AwsCredentialsProvider getCredentialsProvider(final Properties configProps) {
        return getCredentialsProvider(configProps, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
    }

    private static AwsCredentialsProvider getCredentialsProvider(
            final Properties configProps, final String configPrefix) {
        CredentialProvider credentialProviderType =
                AWSUtil.getCredentialProviderType(configProps, configPrefix);

        switch (credentialProviderType) {
            case ENV_VAR:
                return EnvironmentVariableCredentialsProvider.create();

            case SYS_PROP:
                return SystemPropertyCredentialsProvider.create();

            case PROFILE:
                return getProfileCredentialProvider(configProps, configPrefix);

            case BASIC:
                return () ->
                        AwsBasicCredentials.create(
                                configProps.getProperty(
                                        AWSConfigConstants.accessKeyId(configPrefix)),
                                configProps.getProperty(
                                        AWSConfigConstants.secretKey(configPrefix)));

            case ASSUME_ROLE:
                return getAssumeRoleCredentialProvider(configProps, configPrefix);

            case WEB_IDENTITY_TOKEN:
                return getWebIdentityTokenFileCredentialsProvider(
                        WebIdentityTokenFileCredentialsProvider.builder(),
                        configProps,
                        configPrefix);

            case AUTO:
                return DefaultCredentialsProvider.create();

            default:
                throw new IllegalArgumentException(
                        "Credential provider not supported: " + credentialProviderType);
        }
    }

    private static AwsCredentialsProvider getProfileCredentialProvider(
            final Properties configProps, final String configPrefix) {
        String profileName =
                configProps.getProperty(AWSConfigConstants.profileName(configPrefix), null);
        String profileConfigPath =
                configProps.getProperty(AWSConfigConstants.profilePath(configPrefix), null);

        ProfileCredentialsProvider.Builder profileBuilder =
                ProfileCredentialsProvider.builder().profileName(profileName);

        if (profileConfigPath != null) {
            profileBuilder.profileFile(
                    ProfileFile.builder()
                            .type(ProfileFile.Type.CREDENTIALS)
                            .content(Paths.get(profileConfigPath))
                            .build());
        }

        return profileBuilder.build();
    }

    private static AwsCredentialsProvider getAssumeRoleCredentialProvider(
            final Properties configProps, final String configPrefix) {
        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(
                        AssumeRoleRequest.builder()
                                .roleArn(
                                        configProps.getProperty(
                                                AWSConfigConstants.roleArn(configPrefix)))
                                .roleSessionName(
                                        configProps.getProperty(
                                                AWSConfigConstants.roleSessionName(configPrefix)))
                                .externalId(
                                        configProps.getProperty(
                                                AWSConfigConstants.externalId(configPrefix)))
                                .build())
                .stsClient(
                        StsClient.builder()
                                .credentialsProvider(
                                        getCredentialsProvider(
                                                configProps,
                                                AWSConfigConstants.roleCredentialsProvider(
                                                        configPrefix)))
                                .region(getRegion(configProps))
                                .build())
                .build();
    }

    @VisibleForTesting
    static AwsCredentialsProvider getWebIdentityTokenFileCredentialsProvider(
            final WebIdentityTokenFileCredentialsProvider.Builder webIdentityBuilder,
            final Properties configProps,
            final String configPrefix) {

        webIdentityBuilder
                .roleArn(configProps.getProperty(AWSConfigConstants.roleArn(configPrefix), null))
                .roleSessionName(
                        configProps.getProperty(
                                AWSConfigConstants.roleSessionName(configPrefix), null));

        Optional.ofNullable(
                        configProps.getProperty(
                                AWSConfigConstants.webIdentityTokenFile(configPrefix), null))
                .map(Paths::get)
                .ifPresent(webIdentityBuilder::webIdentityTokenFile);

        return webIdentityBuilder.build();
    }

    /**
     * Creates a {@link Region} object from the given Properties.
     *
     * @param configProps the properties containing the region
     * @return the region specified by the properties
     */
    public static Region getRegion(final Properties configProps) {
        return Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION));
    }

    public static boolean isRecoverableException(Exception e) {
        Throwable cause = e.getCause();
        return cause instanceof LimitExceededException
                || cause instanceof ProvisionedThroughputExceededException;
    }

    public static boolean isUsingEfoRecordPublisher(final Properties properties) {
        return EFO.name().equals(properties.get(RECORD_PUBLISHER_TYPE));
    }

    public static boolean isEagerEfoRegistrationType(final Properties properties) {
        return EAGER.name().equals(properties.get(EFO_REGISTRATION_TYPE));
    }

    public static boolean isLazyEfoRegistrationType(final Properties properties) {
        return !isEagerEfoRegistrationType(properties) && !isNoneEfoRegistrationType(properties);
    }

    public static boolean isNoneEfoRegistrationType(final Properties properties) {
        return NONE.name().equals(properties.get(EFO_REGISTRATION_TYPE));
    }
}
