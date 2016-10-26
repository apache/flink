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


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;


@SuppressWarnings("serial")
public class Kafka010ProducerITCase extends KafkaProducerTestBase {

	@Test
	public void testCustomPartitioning() {
		runCustomPartitioningTest();
	}

	/**
	 * Test that the Kafka producer supports a custom Kafka partitioner
	 */
	@Test
	public void testKafkaPartitioner() throws Exception {
		Properties props = new Properties();
		props.putAll(FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings));
		props.putAll(secureProps);
		props.setProperty("partitioner.class", "org.apache.flink.streaming.connectors.kafka.Kafka010ProducerITCase$CustomKafkaPartitioner");
		FlinkKafkaProducer010<String> producer = new FlinkKafkaProducer010<>("topic", new SimpleStringSchema(), props, /* pass no partitioner*/ null);
		producer.setRuntimeContext(new MockRuntimeContext());
		producer.open(new Configuration());
		try {
			producer.invoke("abc");
		} catch(RuntimeException re) {
			if(!re.getMessage().equals("Success")) {
				throw re;
			}
		}
		producer.close();
	}

	public static class CustomKafkaPartitioner implements Partitioner {
		boolean configureCalled = false;
		@Override
		public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
			if(!configureCalled) {
				throw new IllegalArgumentException("Configure has not been called");
			}
			throw new RuntimeException("Success");
		}

		@Override
		public void close() {

		}

		@Override
		public void configure(Map<String, ?> configs) {
			configureCalled = true;
		}
	}

	private static class MockRuntimeContext implements RuntimeContext {
		@Override
		public String getTaskName() {
			return null;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return new UnregisteredTaskMetricsGroup();
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return 2;
		}

		@Override
		public int getIndexOfThisSubtask() {
			return 0;
		}

		@Override
		public int getAttemptNumber() {
			return 0;
		}

		@Override
		public String getTaskNameWithSubtasks() {
			return null;
		}

		@Override
		public ExecutionConfig getExecutionConfig() {
			return null;
		}

		@Override
		public ClassLoader getUserCodeClassLoader() {
			return null;
		}

		@Override
		public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {

		}

		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
			return null;
		}

		@Override
		public Map<String, Accumulator<?, ?>> getAllAccumulators() {
			return null;
		}

		@Override
		public IntCounter getIntCounter(String name) {
			return null;
		}

		@Override
		public LongCounter getLongCounter(String name) {
			return null;
		}

		@Override
		public DoubleCounter getDoubleCounter(String name) {
			return null;
		}

		@Override
		public Histogram getHistogram(String name) {
			return null;
		}

		@Override
		public boolean hasBroadcastVariable(String name) {
			return false;
		}

		@Override
		public <RT> List<RT> getBroadcastVariable(String name) {
			return null;
		}

		@Override
		public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
			return null;
		}

		@Override
		public DistributedCache getDistributedCache() {
			return null;
		}

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			return null;
		}
	}
}
