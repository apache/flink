/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Kafka08 finite source tests.
 */
public class Kafka08FiniteITCase extends KafkaConsumerTestBase {

	@BeforeClass
	public static void prepare() throws ClassNotFoundException {
		// Somehow KafkaConsumer 0.8 doesn't handle broker failures if they are behind a proxy
		prepare(false);
	}

	@Test
	public void testOneToOneFiniteSources() throws Exception {
		runOneToOneTest(true);
	}

	@Ignore
	@Test(timeout =  60000L)
	public void testOneToOneInFiniteSources() throws Exception {
		runOneToOneTest(false);
	}

	@Test
	public void testOneToMultiFiniteSources() throws Exception {
		runOneSourceMultiplePartitionsFiniteTest();
	}

	@Test
	public void testMultiToOneFiniteSources() throws Exception {
		runMultipleSourcesOnePartitionFiniteTest();
	}
}
