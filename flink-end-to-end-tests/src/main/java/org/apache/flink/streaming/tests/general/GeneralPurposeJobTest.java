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

package org.apache.flink.streaming.tests.general;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.tests.general.artificialstate.ArtificialKeyedStateBuilder;
import org.apache.flink.streaming.tests.general.artificialstate.ArtificialKeyedStateMapper;
import org.apache.flink.streaming.tests.general.artificialstate.eventpayload.ArtificialValueStateBuilder;
import org.apache.flink.streaming.tests.general.artificialstate.eventpayload.ComplexPayload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * A general purpose test for Flink.
 */
public class GeneralPurposeJobTest {

	private static final ConfigOption<String> TEST_SEMANTICS = ConfigOptions
		.key("test.semantics")
		.defaultValue("exactly-once")
		.withDescription("This configures the semantics to test. Can be 'exactly-once' or 'at-least-once'");

	private static final ConfigOption<Integer> ENVIRONMENT_PARALLELISM = ConfigOptions
		.key("environment.parallelism")
		.defaultValue(1);

	private static final ConfigOption<Integer> ENVIRONMENT_MAX_PARALLELISM = ConfigOptions
		.key("environment.max_parallelism")
		.defaultValue(128);

	private static final ConfigOption<Integer> ENVIRONMENT_RESTART_DELAY = ConfigOptions
		.key("environment.restart_strategy.delay")
		.defaultValue(0);

	private static final ConfigOption<String> STATE_BACKEND = ConfigOptions
		.key("state_backend")
		.defaultValue("file")
		.withDescription("Supported values are 'file' for FsStateBackend and 'rocks' for RocksDBStateBackend.");

	private static final ConfigOption<String> STATE_BACKEND_CHECKPOINT_DIR = ConfigOptions
		.key("state_backend.checkpoint_directory")
		.noDefaultValue()
		.withDescription("The checkpoint directory.");

	private static final ConfigOption<Boolean> STATE_BACKEND_ROCKS_INCREMENTAL = ConfigOptions
		.key("state_backend.rocks.incremental")
		.defaultValue(false)
		.withDescription("Activate or deactivate incremental snapshots if RocksDBStateBackend is selected.");

	private static final ConfigOption<Boolean> STATE_BACKEND_FILE_ASYNC = ConfigOptions
		.key("state_backend.file.async")
		.defaultValue(true)
		.withDescription("Activate or deactivate asynchronous snapshots if FileStateBackend is selected.");

	private static final ConfigOption<Integer> SEQUENCE_GENERATOR_SRC_KEYSPACE = ConfigOptions
		.key("sequence_generator_source.keyspace")
		.defaultValue(1000);

	private static final ConfigOption<Integer> SEQUENCE_GENERATOR_SRC_PAYLOAD_SIZE = ConfigOptions
		.key("sequence_generator_source.payload_size")
		.defaultValue(20);

	private static final ConfigOption<Long> SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS = ConfigOptions
		.key("sequence_generator_source.event_time.max_out_of_order")
		.defaultValue(500L);

	private static final ConfigOption<Long> SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS = ConfigOptions
		.key("sequence_generator_source.event_time.clock_progress")
		.defaultValue(100L);

	// -----------------------------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		setupEnvironment(env, pt);

		env.addSource(createEventSource(pt))
			.assignTimestampsAndWatermarks(createTimestampExtractor(pt))
			.keyBy(Event::getKey)
			.map(createArtificialKeyedStateMapper(
				(MapFunction<Event, Event>) in -> in,
				(Event first, ComplexPayload second) -> new ComplexPayload(first),
				Arrays.asList(
					new KryoSerializer<>(ComplexPayload.class, env.getConfig()))
//					AvroUtils.getAvroUtils().createAvroSerializer(ComplexPayload.class))
				)
			)
			.returns(Event.class)
			.keyBy(Event::getKey)
			.flatMap(createSemanticsCheckMapper(pt))
//			.timeWindow(Time.seconds(10), Time.seconds(1))
//			.apply(new WindowFunction<Event, Object, Integer, TimeWindow>() {
//				@Override
//				public void apply(Integer integer, TimeWindow window, Iterable<Event> input, Collector<Object> out) throws Exception {
//					System.out.println("------------ "+integer);
//					for (Event event : input) {
//						System.out.println(event);
//					}
//				}
//			});
			.addSink(new PrintSinkFunction<>());

		env.execute("General purpose test job");
	}

	public static void setupEnvironment(StreamExecutionEnvironment env, ParameterTool pt) throws Exception {

		// use event time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// parallelism
		env.setParallelism(pt.getInt(ENVIRONMENT_PARALLELISM.key(), ENVIRONMENT_PARALLELISM.defaultValue()));
		env.setMaxParallelism(pt.getInt(ENVIRONMENT_MAX_PARALLELISM.key(), ENVIRONMENT_MAX_PARALLELISM.defaultValue()));

		// restart strategy
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			Integer.MAX_VALUE,
			pt.getInt(ENVIRONMENT_RESTART_DELAY.key(), ENVIRONMENT_RESTART_DELAY.defaultValue())));

		// state backend
		final String stateBackend = pt.get(
			STATE_BACKEND.key(),
			STATE_BACKEND.defaultValue());

		final String checkpointDir = pt.getRequired(STATE_BACKEND_CHECKPOINT_DIR.key());

		if ("file".equalsIgnoreCase(stateBackend)) {
			boolean asyncCheckpoints = pt.getBoolean(
				STATE_BACKEND_FILE_ASYNC.key(),
				STATE_BACKEND_FILE_ASYNC.defaultValue());

			env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints));
		} else if ("rocks".equalsIgnoreCase(stateBackend)) {
			boolean incrementalCheckpoints = pt.getBoolean(
				STATE_BACKEND_ROCKS_INCREMENTAL.key(),
				STATE_BACKEND_ROCKS_INCREMENTAL.defaultValue());

			env.setStateBackend(new RocksDBStateBackend(checkpointDir, incrementalCheckpoints));
		} else {
			throw new IllegalArgumentException("Unknown backend requested: " + stateBackend);
		}

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(pt);
	}

	private static SourceFunction<Event> createEventSource(ParameterTool pt) {
		return new SequenceGeneratorSource(
			pt.getInt(
				SEQUENCE_GENERATOR_SRC_KEYSPACE.key(),
				SEQUENCE_GENERATOR_SRC_KEYSPACE.defaultValue()),
			pt.getInt(
				SEQUENCE_GENERATOR_SRC_PAYLOAD_SIZE.key(),
				SEQUENCE_GENERATOR_SRC_PAYLOAD_SIZE.defaultValue()),
			pt.getLong(
				SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS.key(),
				SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS.defaultValue()),
			pt.getLong(
				SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS.key(),
				SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS.defaultValue()));
	}

	private static BoundedOutOfOrdernessTimestampExtractor<Event> createTimestampExtractor(ParameterTool pt) {
		return new BoundedOutOfOrdernessTimestampExtractor<Event>(
			Time.milliseconds(
				pt.getLong(
					SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS.key(),
					SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS.defaultValue()))) {

			@Override
			public long extractTimestamp(Event element) {
				return element.getEventTime();
			}
		};
	}

	private static FlatMapFunction<Event, String> createSemanticsCheckMapper(ParameterTool pt) {

		String semantics = pt.get(TEST_SEMANTICS.key(), TEST_SEMANTICS.defaultValue());

		SemanticsCheckMapper.ValidatorFunction validatorFunction;

		if (semantics.equalsIgnoreCase("exactly-once")) {
			validatorFunction = SemanticsCheckMapper.ValidatorFunction.exactlyOnce();
		} else if (semantics.equalsIgnoreCase("at-least-once")) {
			validatorFunction = SemanticsCheckMapper.ValidatorFunction.atLeastOnce();
		} else {
			throw new IllegalArgumentException("Unknown semantics requested: " + semantics);
		}

		return new SemanticsCheckMapper(validatorFunction);
	}

	private static <IN, OUT, STATE> ArtificialKeyedStateMapper<IN, OUT> createArtificialKeyedStateMapper(
		MapFunction<IN, OUT> mapFunction,
		JoinFunction<IN, STATE, STATE> inputAndOldStateToNewState,
		List<TypeSerializer<STATE>> stateSerializers) {

		List<ArtificialKeyedStateBuilder<IN>> artificialStateBuilders = new ArrayList<>(stateSerializers.size());
		for (TypeSerializer<STATE> typeSerializer : stateSerializers) {

			String stateName = "valueState-" + typeSerializer.getClass().getSimpleName() + "-" + UUID.randomUUID();

			ArtificialValueStateBuilder<IN, STATE> stateBuilder = new ArtificialValueStateBuilder<>(
				stateName,
				inputAndOldStateToNewState,
				typeSerializer
			);

			artificialStateBuilders.add(stateBuilder);
		}
		return new ArtificialKeyedStateMapper<>(mapFunction, artificialStateBuilders);
	}
}
