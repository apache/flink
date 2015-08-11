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

package org.apache.flink.streaming.connectors;

import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import org.junit.Test;

import java.util.Properties;


public class Kafka081ITCase extends KafkaConsumerTestBase {
	
	@Override
	protected <T> FlinkKafkaConsumer<T> getConsumer(String topic, DeserializationSchema<T> deserializationSchema, Properties props) {
		return new FlinkKafkaConsumer081<T>(topic, deserializationSchema, props);
	}
	
	// ------------------------------------------------------------------------
	//  Suite of Tests
	// ------------------------------------------------------------------------
	
	@Test
	public void testCheckpointing() {
		runCheckpointingTest();
	}

	@Test
	public void testOffsetInZookeeper() {
		runOffsetInZookeeperValidationTest();
	}
	
	@Test
	public void testConcurrentProducerConsumerTopology() {
		runSimpleConcurrentProducerConsumerTopology();
	}

	// --- canceling / failures ---
	
	@Test
	public void testCancelingEmptyTopic() {
		runCancelingOnEmptyInputTest();
	}

	@Test
	public void testCancelingFullTopic() {
		runCancelingOnFullInputTest();
	}

	@Test
	public void testFailOnDeploy() {
		runFailOnDeployTest();
	}

	// --- source to partition mappings and exactly once ---
	
	@Test
	public void testOneToOneSources() {
		runOneToOneExactlyOnceTest();
	}

	@Test
	public void testOneSourceMultiplePartitions() {
		runOneSourceMultiplePartitionsExactlyOnceTest();
	}

	@Test
	public void testMultipleSourcesOnePartition() {
		runMultipleSourcesOnePartitionExactlyOnceTest();
	}

	// --- broker failure ---

	@Test
	public void testBrokerFailure() {
		runBrokerFailureTest();
	}

	// --- special executions ---
	
	@Test
	public void testBigRecordJob() {
		runBigRecordTestTopology();
	}
}
