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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractSubstituteStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@code StreamConfig}.
 */
public class StreamConfigTest {

	@Test
	public void testGetStreamOperator() {
		// test normal StreamOperator
		{
			StreamConfig streamConfig = new StreamConfig(new Configuration());
			StreamOperator<String> operator = new StreamMap<>(new NoOpMapFunction());
			streamConfig.setStreamOperator(operator);

			assertEquals(operator.getClass(), streamConfig.getStreamOperator(getClass().getClassLoader()).getClass());
		}

		// test substitute StreamOperator
		{
			StreamConfig streamConfig = new StreamConfig(new Configuration());
			StreamOperator<String> actualStreamOperator = mock(StreamOperator.class);
			streamConfig.setStreamOperator(new TestSubstituteStreamOperator<>(actualStreamOperator));

			assertEquals(actualStreamOperator.getClass(), streamConfig.getStreamOperator(getClass().getClassLoader()).getClass());
		}
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class NoOpMapFunction implements MapFunction<String, String> {
		@Override
		public String map(String value) throws Exception {
			return null;
		}
	}

	private static class TestSubstituteStreamOperator<OUT> extends AbstractSubstituteStreamOperator<OUT> {

		private ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;
		private final StreamOperator<OUT> actualStreamOperator;

		TestSubstituteStreamOperator(StreamOperator<OUT> actualStreamOperator) {
			this.actualStreamOperator = actualStreamOperator;
		}

		@Override
		public StreamOperator<OUT> getActualStreamOperator(ClassLoader cl) {
			return actualStreamOperator;
		}

		@Override
		public void setChainingStrategy(ChainingStrategy chainingStrategy) {
			this.chainingStrategy = chainingStrategy;
			this.actualStreamOperator.setChainingStrategy(chainingStrategy);
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return chainingStrategy;
		}
	}
}
