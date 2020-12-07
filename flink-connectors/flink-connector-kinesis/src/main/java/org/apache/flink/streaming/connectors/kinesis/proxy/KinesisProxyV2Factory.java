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
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;
import org.apache.flink.util.Preconditions;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.Properties;

import static java.util.Collections.emptyList;

/**
 * Creates instances of {@link KinesisProxyV2}.
 */
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

		final ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		final SdkAsyncHttpClient httpClient = AwsV2Util.createHttpClient(clientConfiguration, NettyNioAsyncHttpClient.builder(), configProps);
		final FanOutRecordPublisherConfiguration configuration = new FanOutRecordPublisherConfiguration(configProps, emptyList());
		final KinesisAsyncClient client = AwsV2Util.createKinesisAsyncClient(configProps, clientConfiguration, httpClient);

		return new KinesisProxyV2(client, httpClient, configuration, BACKOFF);
	}

}
