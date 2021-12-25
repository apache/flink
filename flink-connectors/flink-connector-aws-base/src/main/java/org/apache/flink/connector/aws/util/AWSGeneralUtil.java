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

package org.apache.flink.connector.aws.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.AttributeMap;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

/** Some general utilities specific to Amazon Web Service. */
@Internal
public class AWSGeneralUtil {
    private static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);
    private static final int INITIAL_WINDOW_SIZE_BYTES = 512 * 1024; // 512 KB
    private static final Duration HEALTH_CHECK_PING_PERIOD = Duration.ofSeconds(60);

    private static final int HTTP_CLIENT_MAX_CONCURRENCY = 10_000;
    private static final Duration HTTP_CLIENT_READ_TIMEOUT = Duration.ofMinutes(6);
    private static final Protocol HTTP_PROTOCOL = Protocol.HTTP2;
    private static final boolean TRUST_ALL_CERTIFICATES = false;
    private static final AttributeMap HTTP_CLIENT_DEFAULTS =
            AttributeMap.builder()
                    .put(SdkHttpConfigurationOption.MAX_CONNECTIONS, HTTP_CLIENT_MAX_CONCURRENCY)
                    .put(SdkHttpConfigurationOption.READ_TIMEOUT, HTTP_CLIENT_READ_TIMEOUT)
                    .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, TRUST_ALL_CERTIFICATES)
                    .put(SdkHttpConfigurationOption.PROTOCOL, HTTP_PROTOCOL)
                    .build();

    /**
     * Determines and returns the credential provider type from the given properties.
     *
     * @return the credential provider type
     */
    public static CredentialProvider getCredentialProviderType(
            final Properties configProps, final String configPrefix) {
        if (!configProps.containsKey(configPrefix)) {
            if (configProps.containsKey(AWSConfigConstants.accessKeyId(configPrefix))
                    && configProps.containsKey(AWSConfigConstants.secretKey(configPrefix))) {
                // if the credential provider type is not specified, but the Access Key ID and
                // Secret Key are given, it will default to BASIC
                return CredentialProvider.BASIC;
            } else {
                // if the credential provider type is not specified, it will default to AUTO
                return CredentialProvider.AUTO;
            }
        } else {
            return CredentialProvider.valueOf(configProps.getProperty(configPrefix));
        }
    }

    /**
     * Return a {@link AwsCredentialsProvider} instance corresponding to the configuration
     * properties.
     *
     * @param configProps the configuration properties
     * @return The corresponding AWS Credentials Provider instance
     */
    public static AwsCredentialsProvider getCredentialsProvider(final Properties configProps) {
        return getCredentialsProvider(configProps, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
    }

    public static AwsCredentialsProvider getCredentialsProvider(
            final Properties configProps, final String configPrefix) {
        CredentialProvider credentialProviderType =
                getCredentialProviderType(configProps, configPrefix);

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

    public static AwsCredentialsProvider getProfileCredentialProvider(
            final Properties configProps, final String configPrefix) {
        String profileName =
                configProps.getProperty(AWSConfigConstants.profileName(configPrefix), null);

        ProfileCredentialsProvider.Builder profileBuilder =
                ProfileCredentialsProvider.builder().profileName(profileName);

        Optional.ofNullable(configProps.getProperty(AWSConfigConstants.profilePath(configPrefix)))
                .map(Paths::get)
                .ifPresent(
                        path ->
                                profileBuilder.profileFile(
                                        ProfileFile.builder()
                                                .type(ProfileFile.Type.CREDENTIALS)
                                                .content(path)
                                                .build()));

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

        Optional.ofNullable(configProps.getProperty(AWSConfigConstants.roleArn(configPrefix)))
                .ifPresent(webIdentityBuilder::roleArn);

        Optional.ofNullable(
                        configProps.getProperty(AWSConfigConstants.roleSessionName(configPrefix)))
                .ifPresent(webIdentityBuilder::roleSessionName);

        Optional.ofNullable(
                        configProps.getProperty(
                                AWSConfigConstants.webIdentityTokenFile(configPrefix)))
                .map(Paths::get)
                .ifPresent(webIdentityBuilder::webIdentityTokenFile);

        return webIdentityBuilder.build();
    }

    public static SdkAsyncHttpClient createAsyncHttpClient(final Properties configProperties) {
        final AttributeMap.Builder clientConfiguration =
                AttributeMap.builder().put(SdkHttpConfigurationOption.TCP_KEEPALIVE, true);

        Optional.ofNullable(
                        configProperties.getProperty(
                                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY))
                .map(Integer::parseInt)
                .ifPresent(
                        integer ->
                                clientConfiguration.put(
                                        SdkHttpConfigurationOption.MAX_CONNECTIONS, integer));

        Optional.ofNullable(
                        configProperties.getProperty(
                                AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS))
                .map(Integer::parseInt)
                .map(Duration::ofMillis)
                .ifPresent(
                        timeout ->
                                clientConfiguration.put(
                                        SdkHttpConfigurationOption.READ_TIMEOUT, timeout));

        Optional.ofNullable(configProperties.getProperty(AWSConfigConstants.TRUST_ALL_CERTIFICATES))
                .map(Boolean::parseBoolean)
                .ifPresent(
                        bool ->
                                clientConfiguration.put(
                                        SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, bool));

        Optional.ofNullable(configProperties.getProperty(AWSConfigConstants.HTTP_PROTOCOL_VERSION))
                .map(Protocol::valueOf)
                .ifPresent(
                        protocol ->
                                clientConfiguration.put(
                                        SdkHttpConfigurationOption.PROTOCOL, protocol));
        return createAsyncHttpClient(
                clientConfiguration.build(), NettyNioAsyncHttpClient.builder());
    }

    public static SdkAsyncHttpClient createAsyncHttpClient(
            final NettyNioAsyncHttpClient.Builder httpClientBuilder) {
        return createAsyncHttpClient(AttributeMap.empty(), httpClientBuilder);
    }

    public static SdkAsyncHttpClient createAsyncHttpClient(
            final AttributeMap config, final NettyNioAsyncHttpClient.Builder httpClientBuilder) {
        httpClientBuilder
                .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
                .http2Configuration(
                        Http2Configuration.builder()
                                .healthCheckPingPeriod(HEALTH_CHECK_PING_PERIOD)
                                .initialWindowSize(INITIAL_WINDOW_SIZE_BYTES)
                                .build());
        return httpClientBuilder.buildWithDefaults(config.merge(HTTP_CLIENT_DEFAULTS));
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

    /**
     * Checks whether or not a region is valid.
     *
     * @param region The AWS region to check
     * @return true if the supplied region is valid, false otherwise
     */
    public static boolean isValidRegion(Region region) {
        return Region.regions().contains(region);
    }

    /**
     * Validates configuration properties related to Amazon AWS service.
     *
     * @param config the properties to setup credentials and region
     */
    public static void validateAwsConfiguration(Properties config) {
        if (config.containsKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER)) {
            // value specified for AWSConfigConstants.AWS_CREDENTIALS_PROVIDER needs to be
            // recognizable
            try {
                getCredentialsProvider(config);
            } catch (IllegalArgumentException e) {
                StringBuilder sb = new StringBuilder();
                for (CredentialProvider type : CredentialProvider.values()) {
                    sb.append(type.toString()).append(", ");
                }
                throw new IllegalArgumentException(
                        "Invalid AWS Credential Provider Type set in config. Valid values are: "
                                + sb.toString());
            }

            // if BASIC type is used, also check that the Access Key ID and Secret Key is supplied
            CredentialProvider credentialsProviderType =
                    getCredentialProviderType(config, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
            if (credentialsProviderType == CredentialProvider.BASIC) {
                if (!config.containsKey(AWSConfigConstants.AWS_ACCESS_KEY_ID)
                        || !config.containsKey(AWSConfigConstants.AWS_SECRET_ACCESS_KEY)) {
                    throw new IllegalArgumentException(
                            "Please set values for AWS Access Key ID ('"
                                    + AWSConfigConstants.AWS_ACCESS_KEY_ID
                                    + "') "
                                    + "and Secret Key ('"
                                    + AWSConfigConstants.AWS_SECRET_ACCESS_KEY
                                    + "') when using the BASIC AWS credential provider type.");
                }
            }
        }

        if (config.containsKey(AWSConfigConstants.AWS_REGION)) {
            // specified AWS Region name must be recognizable
            if (!isValidRegion(getRegion(config))) {
                StringBuilder sb = new StringBuilder();
                for (Region region : Region.regions()) {
                    sb.append(region).append(", ");
                }
                throw new IllegalArgumentException(
                        "Invalid AWS region set in config. Valid values are: " + sb.toString());
            }
        }
    }
}
