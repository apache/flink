/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.avro.generated.Address;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Either;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A simple stateful job that will be used to test avro state evolution and
 * general state migration.
 */
public class StatefulStreamingJob {

	private static final String EXPECTED_DEFAULT_VALUE = "123";

	/**
	 * Stub source that emits one record per second.
	 */
	public static class MySource extends RichParallelSourceFunction<Integer> {

		private static final long serialVersionUID = 1L;

		private volatile boolean cancel;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (!cancel) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(1);
				}
				Thread.sleep(100L);
			}
		}

		@Override
		public void cancel() {
			cancel = true;
		}
	}

	/**
	 * A stateful {@link RichMapFunction} that keeps the required types of state.
	 * That is:
	 * <ol>
	 *     <li>an Avro,</li>
	 *     <li>a Tuple2, and</li>
	 *     <li>an Either type.</li>
	 * </ol>
	 */
	public static class MyStatefulFunction extends RichMapFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		private static final ValueStateDescriptor<Address> AVRO_DESCRIPTOR =
				new ValueStateDescriptor<>("test-state", Address.class);

		private static final ValueStateDescriptor<Tuple2<String, Integer>> TUPLE_DESCRIPTOR =
				new ValueStateDescriptor<>("tuple-state",
						TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

		private static final ValueStateDescriptor<Either<String, Boolean>> EITHER_DESCRIPTOR =
				new ValueStateDescriptor<>("either-state",
						TypeInformation.of(new TypeHint<Either<String, Boolean>>() {}));

		private transient ValueState<Address> avroState;
		private transient ValueState<Tuple2<String, Integer>> tupleState;
		private transient ValueState<Either<String, Boolean>> eitherState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.avroState = getRuntimeContext().getState(AVRO_DESCRIPTOR);
			this.tupleState = getRuntimeContext().getState(TUPLE_DESCRIPTOR);
			this.eitherState = getRuntimeContext().getState(EITHER_DESCRIPTOR);
		}

		@Override
		public String map(Integer value) throws Exception {
			touchState(tupleState, () -> Tuple2.of("19", 19));
			touchState(eitherState, () -> Either.Left("255"));

			final Address newAddress = Address.newBuilder()
					.setCity("New York")
					.setZip("10036")
					.setStreet("555 W 42nd St")
					.setState("NY")
					.setNum(555)
					.build();

			Address existingAddress = avroState.value();
			if (existingAddress != null) {
				if (!Objects.equals(existingAddress.getAppno(), EXPECTED_DEFAULT_VALUE)) {
					// this is expected to fail the job, if found in the output files.
					System.out.println("Wrong Default Value.");
				}
			}
			avroState.update(newAddress);

			return "";
		}

		private static <T> void touchState(ValueState<T> state, Supplier<T> elements) throws IOException {
			T elem = state.value();
			if (elem == null) {
				elem = elements.get();
			}
			state.update(elem);
		}
	}

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);
		final String checkpointDir = pt.getRequired("checkpoint.dir");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new FsStateBackend(checkpointDir));
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.enableCheckpointing(1000L);
		env.getConfig().disableGenericTypes();

		env.addSource(new MySource()).uid("my-source")
				.keyBy(anInt -> 0)
				.map(new MyStatefulFunction()).uid("my-map")
				.addSink(new DiscardingSink<>()).uid("my-sink");
		env.execute();
	}
}
