/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

/**
 * These tests verify that {@link EvictingWindowOperator} correctly interacts with the other
 * windowing components: {@link WindowAssigner},
 * {@link Trigger}.
 * {@link org.apache.flink.streaming.api.functions.windowing.WindowFunction} and window state.
 *
 * <p>These tests document the implicit contract that exists between the windowing components.
 */
public class EvictingWindowOperatorContractTest extends WindowOperatorContractTest {

	protected <W extends Window, OUT> KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
			WindowAssigner<Integer, W> assigner,
			Trigger<Integer, W> trigger,
			long allowedLatenss,
			InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction,
			OutputTag<Integer> lateOutputTag) throws Exception {

		KeySelector<Integer, Integer> keySelector = new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		};

		ListStateDescriptor<StreamRecord<Integer>> intListDescriptor =
				new ListStateDescriptor<>(
						"int-list",
						(TypeSerializer<StreamRecord<Integer>>) new StreamElementSerializer(IntSerializer.INSTANCE));

		@SuppressWarnings("unchecked")
		EvictingWindowOperator<Integer, Integer, OUT, W> operator = new EvictingWindowOperator<>(
				assigner,
				assigner.getWindowSerializer(new ExecutionConfig()),
				keySelector,
				IntSerializer.INSTANCE,
				intListDescriptor,
				windowFunction,
				trigger,
				CountEvictor.<W>of(100),
				allowedLatenss,
				lateOutputTag);

		return new KeyedOneInputStreamOperatorTestHarness<>(
				operator,
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);
	}

	protected <W extends Window, OUT> KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
			WindowAssigner<Integer, W> assigner,
			Trigger<Integer, W> trigger,
			long allowedLatenss,
			InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction) throws Exception {

		return createWindowOperator(
				assigner,
				trigger,
				allowedLatenss,
				windowFunction,
				null /* late output tag */);
	}
}
