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
import org.apache.flink.runtime.util.EnvironmentInformation;

import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.net.URI;
import java.util.Optional;
import java.util.Properties;

/** Some utilities specific to Amazon Web Service. */
@Internal
public class AWSAsyncSinkUtil extends AWSGeneralUtil {

    /** V2 suffix to denote the unified sinks. V1 sinks are based on KPL etc. */
    static final String V2_USER_AGENT_SUFFIX = " V2";

    /**
     * Creates a user agent prefix for Flink. This can be used by HTTP Clients.
     *
     * @param userAgentFormat flink user agent prefix format with placeholders for version and
     *     commit id.
     * @return a user agent prefix for Flink
     */
    public static String formatFlinkUserAgentPrefix(String userAgentFormat) {
        return String.format(
                userAgentFormat,
                EnvironmentInformation.getVersion(),
                EnvironmentInformation.getRevisionInformation().commitId);
    }

    /**
     * @param configProps configuration properties
     * @param httpClient the underlying HTTP client used to talk to AWS
     * @return a new AWS Client
     */
    public static <
                    S extends SdkClient,
                    T extends
                            AwsAsyncClientBuilder<? extends T, S>
                                    & AwsClientBuilder<? extends T, S>>
            S createAwsAsyncClient(
                    final Properties configProps,
                    final SdkAsyncHttpClient httpClient,
                    final T clientBuilder,
                    final String awsUserAgentPrefixFormat,
                    final String awsClientUserAgentPrefix) {
        SdkClientConfiguration clientConfiguration = SdkClientConfiguration.builder().build();
        return createAwsAsyncClient(
                configProps,
                clientConfiguration,
                httpClient,
                clientBuilder,
                awsUserAgentPrefixFormat,
                awsClientUserAgentPrefix);
    }

    /**
     * @param configProps configuration properties
     * @param clientConfiguration the AWS SDK v2 config to instantiate the client
     * @param httpClient the underlying HTTP client used to talk to AWS
     * @return a new AWS Client
     */
    public static <
                    S extends SdkClient,
                    T extends
                            AwsAsyncClientBuilder<? extends T, S>
                                    & AwsClientBuilder<? extends T, S>>
            S createAwsAsyncClient(
                    final Properties configProps,
                    final SdkClientConfiguration clientConfiguration,
                    final SdkAsyncHttpClient httpClient,
                    final T clientBuilder,
                    final String awsUserAgentPrefixFormat,
                    final String awsClientUserAgentPrefix) {
        String flinkUserAgentPrefix =
                Optional.ofNullable(configProps.getProperty(awsClientUserAgentPrefix))
                        .orElse(
                                formatFlinkUserAgentPrefix(
                                        awsUserAgentPrefixFormat + V2_USER_AGENT_SUFFIX));

        final ClientOverrideConfiguration overrideConfiguration =
                createClientOverrideConfiguration(
                        clientConfiguration,
                        ClientOverrideConfiguration.builder(),
                        flinkUserAgentPrefix);

        return createAwsAsyncClient(configProps, clientBuilder, httpClient, overrideConfiguration);
    }

    @VisibleForTesting
    static ClientOverrideConfiguration createClientOverrideConfiguration(
            final SdkClientConfiguration config,
            final ClientOverrideConfiguration.Builder overrideConfigurationBuilder,
            String flinkUserAgentPrefix) {

        overrideConfigurationBuilder
                .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, flinkUserAgentPrefix)
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_SUFFIX,
                        config.option(SdkAdvancedClientOption.USER_AGENT_SUFFIX));

        Optional.ofNullable(config.option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT))
                .ifPresent(overrideConfigurationBuilder::apiCallAttemptTimeout);

        Optional.ofNullable(config.option(SdkClientOption.API_CALL_TIMEOUT))
                .ifPresent(overrideConfigurationBuilder::apiCallTimeout);

        return overrideConfigurationBuilder.build();
    }

    @VisibleForTesting
    static <
                    S extends SdkClient,
                    T extends
                            AwsAsyncClientBuilder<? extends T, S>
                                    & AwsClientBuilder<? extends T, S>>
            S createAwsAsyncClient(
                    final Properties configProps,
                    final T clientBuilder,
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
}
