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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.config.AWSKinesisDataStreamsConfigConstants;
import org.apache.flink.connector.kinesis.util.AWSKinesisDataStreamsUtil;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.util.Properties;

import static java.util.Collections.emptyList;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TCP_KEEPALIVE;

/** Creates instances of {@link KinesisProxyV2}. */
@Internal
public class KinesisProxyV2Factory {

    private static final FullJitterBackoff BACKOFF = new FullJitterBackoff();

    /**
     * Uses the given properties to instantiate a new instance of {@link KinesisProxyV2}.
     *
     * @param configProps the properties used to parse configuration
     * @return the Kinesis proxy
     */
    public static KinesisProxyV2Interface createKinesisProxyV2(final Properties configProps) {
        Preconditions.checkNotNull(configProps);

        final AttributeMap convertedProperties = AwsV2Util.convertProperties(configProps);
        final AttributeMap.Builder clientConfiguration = AttributeMap.builder();
        populateDefaultValues(clientConfiguration);

        final SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(
                        convertedProperties.merge(clientConfiguration.build()),
                        NettyNioAsyncHttpClient.builder());
        final FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(configProps, emptyList());

        Properties legacyConfigProps = new Properties(configProps);
        legacyConfigProps.setProperty(
                AWSKinesisDataStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX,
                AWSKinesisDataStreamsUtil.formatFlinkUserAgentPrefix(
                        AWSKinesisDataStreamsConfigConstants
                                .BASE_KINESIS_USER_AGENT_PREFIX_FORMAT));

        final KinesisAsyncClient client =
                AWSKinesisDataStreamsUtil.createKinesisAsyncClient(legacyConfigProps, httpClient);

        return new KinesisProxyV2(client, httpClient, configuration, BACKOFF);
    }

    private static void populateDefaultValues(final AttributeMap.Builder clientConfiguration) {
        clientConfiguration.put(TCP_KEEPALIVE, true);
    }
}
