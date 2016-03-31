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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;


public class CEPOperatorTest extends TestLogger {

	@Test
	public void testCEPOperatorWatermarkForwarding() throws Exception {
		OneInputStreamOperatorTestHarness<Integer, Map<String, Integer>> harness = new OneInputStreamOperatorTestHarness<>(
			new CEPPatternOperator<Integer>(
				IntSerializer.INSTANCE,
				false,
				new DummyNFAFactory<>(IntSerializer.INSTANCE))
		);

		harness.open();

		Watermark expectedWatermark = new Watermark(42L);

		harness.processWatermark(expectedWatermark);

		Object watermark = harness.getOutput().poll();

		assertTrue(watermark instanceof Watermark);
		assertEquals(expectedWatermark, watermark);

		harness.close();
	}

	@Test
	public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {
		KeySelector<Integer, Integer> keySelector = new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		};

		OneInputStreamOperatorTestHarness<Integer, Map<String, Integer>> harness = new OneInputStreamOperatorTestHarness<>(
			new KeyedCEPPatternOperator<Integer, Integer>(
				IntSerializer.INSTANCE,
				false,
				keySelector,
				IntSerializer.INSTANCE,
			new DummyNFAFactory<>(IntSerializer.INSTANCE))
		);

		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);

		harness.open();

		Watermark expectedWatermark = new Watermark(42L);

		harness.processWatermark(expectedWatermark);

		Object watermark = harness.getOutput().poll();

		assertTrue(watermark instanceof Watermark);
		assertEquals(expectedWatermark, watermark);

		harness.close();
	}

	public static class DummyNFAFactory<T> implements NFACompiler.NFAFactory<T> {

		private static final long serialVersionUID = 1173020762472766713L;

		private final TypeSerializer<T> inputTypeSerializer;

		public DummyNFAFactory(TypeSerializer<T> inputTypeSerializer) {
			this.inputTypeSerializer = inputTypeSerializer;
		}

		@Override
		public NFA<T> createNFA() {
			return new NFA<>(inputTypeSerializer.duplicate(), 0);
		}
	}
}
