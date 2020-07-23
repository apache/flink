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
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.Properties;

/**
 * Kinesis proxy implementation using AWS SDK v2.x - a utility class that is used as a proxy to make
 * calls to AWS Kinesis for several EFO (Enhanced Fan Out) functions, such as de-/registering stream consumers,
 * subscribing to a shard and receiving records from a shard.
 */
@Internal
public class KinesisProxyV2 implements KinesisProxyV2Interface {

	private final KinesisAsyncClient kinesisAsyncClient;

	/**
	 * Create a new KinesisProxyV2 based on the supplied configuration properties.
	 *
	 * @param configProps configuration properties containing AWS credential and AWS region info
	 */
	public KinesisProxyV2(final Properties configProps) {
		this.kinesisAsyncClient = createKinesisAsyncClient(configProps);
	}

	/**
	 * Create the Kinesis client, using the provided configuration properties.
	 * Derived classes can override this method to customize the client configuration.
	 *
	 * @param configProps the properties map used to create the Kinesis Client
	 * @return a Kinesis Client
	 */
	protected KinesisAsyncClient createKinesisAsyncClient(final Properties configProps) {
		final ClientConfiguration config = new ClientConfigurationFactory().getConfig();
		return AwsV2Util.createKinesisAsyncClient(configProps, config);
	}

}
