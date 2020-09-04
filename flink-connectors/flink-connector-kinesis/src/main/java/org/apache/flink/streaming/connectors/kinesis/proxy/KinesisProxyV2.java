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
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

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
	 * @param kinesisAsyncClient the kinesis async client used to communicate with Kinesis
	 */
	public KinesisProxyV2(final KinesisAsyncClient kinesisAsyncClient) {
		this.kinesisAsyncClient = Preconditions.checkNotNull(kinesisAsyncClient);
	}

}
