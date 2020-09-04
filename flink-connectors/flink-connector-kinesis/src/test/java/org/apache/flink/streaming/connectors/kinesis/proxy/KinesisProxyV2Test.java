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

import org.junit.Test;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link KinesisProxyV2}.
 */
public class KinesisProxyV2Test {

	@Test
	public void testSubscribeToShard() {
		KinesisAsyncClient kinesis = mock(KinesisAsyncClient.class);
		KinesisProxyV2 proxy = new KinesisProxyV2(kinesis);

		SubscribeToShardRequest request = SubscribeToShardRequest.builder().build();
		SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
			.builder()
			.subscriber(event -> {})
			.build();

		proxy.subscribeToShard(request, responseHandler);

		verify(kinesis).subscribeToShard(eq(request), eq(responseHandler));
	}

	@Test
	public void testCloseInvokesClientClose() {
		KinesisAsyncClient kinesis = mock(KinesisAsyncClient.class);
		KinesisProxyV2 proxy = new KinesisProxyV2(kinesis);

		proxy.close();

		verify(kinesis).close();
	}

}
