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

package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.CredentialProvider;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

/** Utility methods specific to Amazon Web Service SDK v2.x. */
@Internal
public class AwsV2Util {

    /** Used for formatting Flink-specific user agent string when creating DynamoDb client. */
    private static final String USER_AGENT_FORMAT = "Apache Flink %s (%s) DynamoDb Connector";

    /**
     * Creates an Amazon DynamoDb Client from the provided properties.
     *
     * @param configProps configuration properties
     * @return a new Amazon DynamoDb Client
     */
    public static DynamoDbClient createDynamoDbClient(final Properties configProps) {
        final DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();
        final ClientOverrideConfiguration overrideConfiguration =
                createClientOverrideConfiguration(ClientOverrideConfiguration.builder());
        return createDynamoDbClient(configProps, clientBuilder, overrideConfiguration);
    }

    @VisibleForTesting
    static DynamoDbClient createDynamoDbClient(
            final Properties configProps,
            final DynamoDbClientBuilder clientBuilder,
            final ClientOverrideConfiguration overrideConfiguration) {

        if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
            final URI endpointOverride =
                    URI.create(configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT));
            clientBuilder.endpointOverride(endpointOverride);
        }

        return clientBuilder
                .credentialsProvider(getCredentialsProvider(configProps))
                .region(getRegion(configProps))
                .overrideConfiguration(overrideConfiguration)
                .build();
    }

    @VisibleForTesting
    static ClientOverrideConfiguration createClientOverrideConfiguration(
            final ClientOverrideConfiguration.Builder overrideConfigurationBuilder) {

        overrideConfigurationBuilder.putAdvancedOption(
                SdkAdvancedClientOption.USER_AGENT_PREFIX, formatFlinkUserAgentPrefix());

        return overrideConfigurationBuilder.build();
    }

    /**
     * Creates a user agent prefix for Flink. This can be used by HTTP Clients.
     *
     * @return a user agent prefix for Flink
     */
    public static String formatFlinkUserAgentPrefix() {
        return String.format(
                USER_AGENT_FORMAT,
                EnvironmentInformation.getVersion(),
                EnvironmentInformation.getRevisionInformation().commitId);
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

    private static AwsCredentialsProvider getCredentialsProvider(
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

    /**
     * Determines and returns the credential provider type from the given properties.
     *
     * @return the credential provider type
     */
    static CredentialProvider getCredentialProviderType(
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
     * Creates a {@link Region} object from the given Properties.
     *
     * @param configProps the properties containing the region
     * @return the region specified by the properties
     */
    public static Region getRegion(final Properties configProps) {
        return Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION));
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
}
