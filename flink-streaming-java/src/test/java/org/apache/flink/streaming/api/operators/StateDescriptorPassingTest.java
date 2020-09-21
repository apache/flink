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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.util.Collector;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 * Various tests around the proper passing of state descriptors to the operators
 * and their serialization.
 *
 * <p>The tests use an arbitrary generic type to validate the behavior.
 */
@SuppressWarnings("serial")
public class StateDescriptorPassingTest {

	@Test
	public void testReduceWindowState() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(File.class, JavaSerializer.class);

		DataStream<File> src = env.fromElements(new File("/"))
				.assignTimestampsAndWatermarks(WatermarkStrategy.<File>forMonotonousTimestamps()
						.withTimestampAssigner((file, ts) -> System.currentTimeMillis()));

		SingleOutputStreamOperator<?> result = src
				.keyBy(new KeySelector<File, String>() {
					@Override
					public String getKey(File value) {
						return null;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
				.reduce(new ReduceFunction<File>() {

					@Override
					public File reduce(File value1, File value2) {
						return null;
					}
				});

		validateStateDescriptorConfigured(result);
	}

	@Test
	public void testApplyWindowState() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(File.class, JavaSerializer.class);

		DataStream<File> src = env.fromElements(new File("/"))
				.assignTimestampsAndWatermarks(WatermarkStrategy.<File>forMonotonousTimestamps()
						.withTimestampAssigner((file, ts) -> System.currentTimeMillis()));

		SingleOutputStreamOperator<?> result = src
				.keyBy(new KeySelector<File, String>() {
					@Override
					public String getKey(File value) {
						return null;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
				.apply(new WindowFunction<File, String, String, TimeWindow>() {
					@Override
					public void apply(String s, TimeWindow window,
										Iterable<File> input, Collector<String> out) {}
				});

		validateListStateDescriptorConfigured(result);
	}

	@Test
	public void testProcessWindowState() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(File.class, JavaSerializer.class);

		DataStream<File> src = env.fromElements(new File("/"))
				.assignTimestampsAndWatermarks(WatermarkStrategy.<File>forMonotonousTimestamps()
						.withTimestampAssigner((file, ts) -> System.currentTimeMillis()));

		SingleOutputStreamOperator<?> result = src
				.keyBy(new KeySelector<File, String>() {
					@Override
					public String getKey(File value) {
						return null;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
				.process(new ProcessWindowFunction<File, String, String, TimeWindow>() {
					@Override
					public void process(String s, Context ctx,
							Iterable<File> input, Collector<String> out) {}
				});

		validateListStateDescriptorConfigured(result);
	}

	@Test
	public void testProcessAllWindowState()  {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(File.class, JavaSerializer.class);

		// simulate ingestion time
		DataStream<File> src = env.fromElements(new File("/"))
				.assignTimestampsAndWatermarks(WatermarkStrategy.<File>forMonotonousTimestamps()
						.withTimestampAssigner((file, ts) -> System.currentTimeMillis()));

		SingleOutputStreamOperator<?> result = src
				.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
				.process(new ProcessAllWindowFunction<File, String, TimeWindow>() {
					@Override
					public void process(Context ctx, Iterable<File> input, Collector<String> out) {}
				});

		validateListStateDescriptorConfigured(result);
	}

	@Test
	public void testReduceWindowAllState() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(File.class, JavaSerializer.class);

		// simulate ingestion time
		DataStream<File> src = env.fromElements(new File("/"))
				.assignTimestampsAndWatermarks(WatermarkStrategy.<File>forMonotonousTimestamps()
						.withTimestampAssigner((file, ts) -> System.currentTimeMillis()));

		SingleOutputStreamOperator<?> result = src
				.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
				.reduce(new ReduceFunction<File>() {

					@Override
					public File reduce(File value1, File value2) {
						return null;
					}
				});

		validateStateDescriptorConfigured(result);
	}

	@Test
	public void testApplyWindowAllState() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(File.class, JavaSerializer.class);

		// simulate ingestion time
		DataStream<File> src = env.fromElements(new File("/"))
				.assignTimestampsAndWatermarks(WatermarkStrategy.<File>forMonotonousTimestamps()
						.withTimestampAssigner((file, ts) -> System.currentTimeMillis()));

		SingleOutputStreamOperator<?> result = src
				.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
				.apply(new AllWindowFunction<File, String, TimeWindow>() {
					@Override
					public void apply(TimeWindow window, Iterable<File> input, Collector<String> out) {}
				});

		validateListStateDescriptorConfigured(result);
	}

	// ------------------------------------------------------------------------
	//  generic validation
	// ------------------------------------------------------------------------

	private void validateStateDescriptorConfigured(SingleOutputStreamOperator<?> result) {
		OneInputTransformation<?, ?> transform = (OneInputTransformation<?, ?>) result.getTransformation();
		WindowOperator<?, ?, ?, ?, ?> op = (WindowOperator<?, ?, ?, ?, ?>) transform.getOperator();
		StateDescriptor<?, ?> descr = op.getStateDescriptor();

		// this would be the first statement to fail if state descriptors were not properly initialized
		TypeSerializer<?> serializer = descr.getSerializer();
		assertTrue(serializer instanceof KryoSerializer);

		Kryo kryo = ((KryoSerializer<?>) serializer).getKryo();

		assertTrue("serializer registration was not properly passed on",
				kryo.getSerializer(File.class) instanceof JavaSerializer);
	}

	private void validateListStateDescriptorConfigured(SingleOutputStreamOperator<?> result) {
		OneInputTransformation<?, ?> transform = (OneInputTransformation<?, ?>) result.getTransformation();
		WindowOperator<?, ?, ?, ?, ?> op = (WindowOperator<?, ?, ?, ?, ?>) transform.getOperator();
		StateDescriptor<?, ?> descr = op.getStateDescriptor();

		assertTrue(descr instanceof ListStateDescriptor);

		ListStateDescriptor<?> listDescr = (ListStateDescriptor<?>) descr;

		// this would be the first statement to fail if state descriptors were not properly initialized
		TypeSerializer<?> serializer = listDescr.getSerializer();
		assertTrue(serializer instanceof ListSerializer);

		TypeSerializer<?> elementSerializer = listDescr.getElementSerializer();
		assertTrue(elementSerializer instanceof KryoSerializer);

		Kryo kryo = ((KryoSerializer<?>) elementSerializer).getKryo();

		assertTrue("serializer registration was not properly passed on",
				kryo.getSerializer(File.class) instanceof JavaSerializer);
	}
}
