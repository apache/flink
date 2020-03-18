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

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.util.Collector;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link MapBundleOperator}.
 */
public class MapBundleOperatorTest {

	@Test
	public void testSimple() throws Exception {
		@SuppressWarnings("unchecked")
		TestMapBundleFunction func = new TestMapBundleFunction();
		CountBundleTrigger<Tuple2<String, String>> trigger = new CountBundleTrigger<>(3);
		KeySelector<Tuple2<String, String>, String> keySelector =
				(KeySelector<Tuple2<String, String>, String>) value -> value.f0;

		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> op =
				new OneInputStreamOperatorTestHarness<>(
						new MapBundleOperator<>(func, trigger, keySelector));
		op.open();
		synchronized (op.getCheckpointLock()) {
			StreamRecord<Tuple2<String, String>> input = new StreamRecord<>(null);

			input.replace(new Tuple2<>("k1", "v1"));
			op.processElement(input);

			input.replace(new Tuple2<>("k1", "v2"));
			op.processElement(input);

			assertEquals(0, func.getFinishCount());

			input.replace(new Tuple2<>("k2", "v3"));
			op.processElement(input);

			assertEquals(1, func.getFinishCount());
			assertThat(Arrays.asList("k1=v1,v2", "k2=v3"), is(func.getOutputs()));

			input.replace(new Tuple2<>("k3", "v4"));
			op.processElement(input);

			input.replace(new Tuple2<>("k4", "v5"));
			op.processElement(input);

			assertEquals(1, func.getFinishCount());

			op.close();
			assertEquals(2, func.getFinishCount());
			assertThat(Arrays.asList("k3=v4", "k4=v5"), is(func.getOutputs()));
		}
	}

	private static class TestMapBundleFunction extends MapBundleFunction<String, String, Tuple2<String, String>, String> {

		private int finishCount = 0;
		private List<String> outputs = new ArrayList<>();

		@Override
		public String addInput(@Nullable String value, Tuple2<String, String> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				return value + "," + input.f1;
			}
		}

		@Override
		public void finishBundle(Map<String, String> buffer, Collector<String> out) throws Exception {
			finishCount++;
			outputs.clear();
			for (Map.Entry<String, String> entry : buffer.entrySet()) {
				outputs.add(entry.getKey() + "=" + entry.getValue());
			}
		}

		int getFinishCount() {
			return finishCount;
		}

		List<String> getOutputs() {
			return outputs;
		}
	}
}
