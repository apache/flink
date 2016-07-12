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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class Kafka010ITCase extends KafkaConsumerTestBase {

	// ------------------------------------------------------------------------
	//  Suite of Tests
	// ------------------------------------------------------------------------

	@Override
	public String getExpectedKafkaVersion() {
		return "0.10";
	}

	@Test(timeout = 60000)
	public void testFailOnNoBroker() throws Exception {
		runFailOnNoBrokerTest();
	}

	@Test(timeout = 60000)
	public void testConcurrentProducerConsumerTopology() throws Exception {
		runSimpleConcurrentProducerConsumerTopology();
	}

//	@Test(timeout = 60000)
//	public void testPunctuatedExplicitWMConsumer() throws Exception {
//		runExplicitPunctuatedWMgeneratingConsumerTest(false);
//	}

//	@Test(timeout = 60000)
//	public void testPunctuatedExplicitWMConsumerWithEmptyTopic() throws Exception {
//		runExplicitPunctuatedWMgeneratingConsumerTest(true);
//	}

	@Test(timeout = 60000)
	public void testKeyValueSupport() throws Exception {
		runKeyValueTest();
	}

	// --- canceling / failures ---

	@Test(timeout = 60000)
	public void testCancelingEmptyTopic() throws Exception {
		runCancelingOnEmptyInputTest();
	}

	@Test(timeout = 60000)
	public void testCancelingFullTopic() throws Exception {
		runCancelingOnFullInputTest();
	}

	@Test(timeout = 60000)
	public void testFailOnDeploy() throws Exception {
		runFailOnDeployTest();
	}


	// --- source to partition mappings and exactly once ---

	@Test(timeout = 60000)
	public void testOneToOneSources() throws Exception {
		runOneToOneExactlyOnceTest();
	}

	@Test(timeout = 60000)
	public void testOneSourceMultiplePartitions() throws Exception {
		runOneSourceMultiplePartitionsExactlyOnceTest();
	}

	@Test(timeout = 60000)
	public void testMultipleSourcesOnePartition() throws Exception {
		runMultipleSourcesOnePartitionExactlyOnceTest();
	}

	// --- broker failure ---

	@Test(timeout = 60000)
	public void testBrokerFailure() throws Exception {
		runBrokerFailureTest();
	}

	// --- special executions ---

	@Test(timeout = 60000)
	public void testBigRecordJob() throws Exception {
		runBigRecordTestTopology();
	}

	@Test(timeout = 60000)
	public void testMultipleTopics() throws Exception {
		runProduceConsumeMultipleTopics();
	}

	@Test(timeout = 60000)
	public void testAllDeletes() throws Exception {
		runAllDeletesTest();
	}

	@Test(timeout = 60000)
	public void testMetricsAndEndOfStream() throws Exception {
		runMetricsAndEndOfStreamTest();
	}

	@Test
	public void testJsonTableSource() throws Exception {
		String topic = UUID.randomUUID().toString();

		// Names and types are determined in the actual test method of the
		// base test class.
		Kafka010JsonTableSource tableSource = new Kafka010JsonTableSource(
				topic,
				standardProps,
				new String[] {
						"long",
						"string",
						"boolean",
						"double",
						"missing-field"},
				new TypeInformation<?>[] {
						BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.BOOLEAN_TYPE_INFO,
						BasicTypeInfo.DOUBLE_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO });

		// Don't fail on missing field, but set to null (default)
		tableSource.setFailOnMissingField(false);

		runJsonTableSource(topic, tableSource);
	}

	@Test
	public void testJsonTableSourceWithFailOnMissingField() throws Exception {
		String topic = UUID.randomUUID().toString();

		// Names and types are determined in the actual test method of the
		// base test class.
		Kafka010JsonTableSource tableSource = new Kafka010JsonTableSource(
				topic,
				standardProps,
				new String[] {
						"long",
						"string",
						"boolean",
						"double",
						"missing-field"},
				new TypeInformation<?>[] {
						BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.BOOLEAN_TYPE_INFO,
						BasicTypeInfo.DOUBLE_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO });

		// Don't fail on missing field, but set to null (default)
		tableSource.setFailOnMissingField(true);

		try {
			runJsonTableSource(topic, tableSource);
			fail("Did not throw expected Exception");
		} catch (Exception e) {
			Throwable rootCause = e.getCause().getCause().getCause();
			assertTrue("Unexpected root cause", rootCause instanceof IllegalStateException);
		}
	}

}
