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

package org.apache.flink.streaming.connectors.rocketmq;

import org.apache.flink.streaming.connectors.rocketmq.common.ProducerConfig;

import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;

/**
 * Tests for the {@link RocketMQSink}.
 */
public class RocketMQSinkTest {

	private ProducerConfig producerConfig = null;

	@Before
	public void before() throws Exception {

		producerConfig = new ProducerConfig();
		producerConfig.setProducerGroupName("test_sink_producer");
		producerConfig.setNameServerAddress("localhost:9876");
	}

	@Test
	public void testCreateRocketMQSink() throws Exception {

		RocketMQSink rocketMQSink = new RocketMQSink(producerConfig);
		Assert.assertNotNull(rocketMQSink);
	}
}
