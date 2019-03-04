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
import org.junit.Test;

/**
 * IT cases for the {@link FlinkKafkaProducer}.
 */
@SuppressWarnings("serial")
public class KafkaProducerExactlyOnceITCase extends KafkaProducerTestBase {
	@BeforeClass
	public static void prepare() throws ClassNotFoundException {
		KafkaProducerTestBase.prepare();
		((KafkaTestEnvironmentImpl) kafkaServer).setProducerSemantic(FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
	}

	@Override
	public void testOneToOneAtLeastOnceRegularSink() throws Exception {
		// TODO: fix this test
		// currently very often (~50% cases) KafkaProducer live locks itself on commitTransaction call.
		// Somehow Kafka 10 doesn't play along with NetworkFailureProxy. This can either mean a bug in Kafka
		// that it doesn't work well with some weird network failures, or the NetworkFailureProxy is a broken design
		// and this test should be reimplemented in completely different way...
	}

	@Override
	public void testOneToOneAtLeastOnceCustomOperator() throws Exception {
		// TODO: fix this test
		// currently very often (~50% cases) KafkaProducer live locks itself on commitTransaction call.
		// Somehow Kafka 10 doesn't play along with NetworkFailureProxy. This can either mean a bug in Kafka
		// that it doesn't work well with some weird network failures, or the NetworkFailureProxy is a broken design
		// and this test should be reimplemented in completely different way...
	}

	@Test
	public void testMultipleSinkOperators() throws Exception {
		testExactlyOnce(false, 2);
	}
}
