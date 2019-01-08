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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.tests.artificialstate.ComplexPayload;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createArtificialKeyedStateMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createEventSource;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createSemanticsCheckMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createTimestampExtractor;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * Test upgrade of generic stateful job for Flink's DataStream API operators and primitives.
 *
 * <p>The job is constructed of generic components from {@link DataStreamAllroundTestJobFactory}.
 * The goal is to test successful state restoration after taking savepoint and recovery with new job version.
 * It can be configured with '--test.job.variant' to run different variants of it:
 * <ul>
 *     <li><b>original:</b> includes 2 custom stateful map operators</li>
 *     <li><b>upgraded:</b> changes order of 2 custom stateful map operators and adds one more</li>
 * </ul>
 *
 * <p>The cli job configuration options are described in {@link DataStreamAllroundTestJobFactory}.
 *
 * <p>Job specific configuration options:
 * <ul>
 *     <li>test.job.variant (String, default - 'original'): This configures the job variant to test. Can be 'original' or 'upgraded'.</li>
 * </ul>
 *
 */
public class StatefulStreamJobUpgradeTestProgram {
	private static final String TEST_JOB_VARIANT_ORIGINAL = "original";
	private static final String TEST_JOB_VARIANT_UPGRADED = "upgraded";

	private static final ConfigOption<String> TEST_JOB_VARIANT = ConfigOptions
		.key("test.job.variant")
		.defaultValue(TEST_JOB_VARIANT_ORIGINAL)
		.withDescription(String.format("This configures the job variant to test. Can be '%s' or '%s'",
			TEST_JOB_VARIANT_ORIGINAL, TEST_JOB_VARIANT_UPGRADED));

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		setupEnvironment(env, pt);

		KeyedStream<Event, Integer> source = env.addSource(createEventSource(pt))
			.name("EventSource")
			.uid("EventSource")
			.assignTimestampsAndWatermarks(createTimestampExtractor(pt))
			.keyBy(Event::getKey);

		List<TypeSerializer<ComplexPayload>> stateSer =
			Collections.singletonList(new KryoSerializer<>(ComplexPayload.class, env.getConfig()));

		KeyedStream<Event, Integer> afterStatefulOperations = isOriginalJobVariant(pt) ?
			applyOriginalStatefulOperations(source, stateSer, Collections.emptyList()) :
			applyUpgradedStatefulOperations(source, stateSer, Collections.emptyList());

		afterStatefulOperations
			.flatMap(createSemanticsCheckMapper(pt))
			.name("SemanticsCheckMapper")
			.addSink(new PrintSinkFunction<>());

		env.execute("General purpose test job");
	}

	private static boolean isOriginalJobVariant(final ParameterTool pt) {
		switch (pt.get(TEST_JOB_VARIANT.key())) {
			case TEST_JOB_VARIANT_ORIGINAL:
				return true;
			case TEST_JOB_VARIANT_UPGRADED:
				return false;
			default:
				throw new IllegalArgumentException(String.format("'--test.job.variant' can be either '%s' or '%s'",
					TEST_JOB_VARIANT_ORIGINAL, TEST_JOB_VARIANT_UPGRADED));
		}
	}

	private static KeyedStream<Event, Integer> applyOriginalStatefulOperations(
		KeyedStream<Event, Integer> source,
		List<TypeSerializer<ComplexPayload>> stateSer,
		List<Class<ComplexPayload>> stateClass) {
		source = applyTestStatefulOperator("stateMap1", simpleStateUpdate("stateMap1"), source, stateSer, stateClass);
		return applyTestStatefulOperator("stateMap2", lastStateUpdate("stateMap2"), source, stateSer, stateClass);
	}

	private static KeyedStream<Event, Integer> applyUpgradedStatefulOperations(
		KeyedStream<Event, Integer> source,
		List<TypeSerializer<ComplexPayload>> stateSer,
		List<Class<ComplexPayload>> stateClass) {
		source = applyTestStatefulOperator("stateMap2", simpleStateUpdate("stateMap2"), source, stateSer, stateClass);
		source = applyTestStatefulOperator("stateMap1", lastStateUpdate("stateMap1"), source, stateSer, stateClass);
		return applyTestStatefulOperator("stateMap3", simpleStateUpdate("stateMap3"), source, stateSer, stateClass);
	}

	private static KeyedStream<Event, Integer> applyTestStatefulOperator(
		String name,
		JoinFunction<Event, ComplexPayload, ComplexPayload> stateFunc,
		KeyedStream<Event, Integer> source,
		List<TypeSerializer<ComplexPayload>> stateSer,
		List<Class<ComplexPayload>> stateClass) {
		return source
			.map(createArtificialKeyedStateMapper(e -> e, stateFunc, stateSer, stateClass))
			.name(name)
			.uid(name)
			.returns(Event.class)
			.keyBy(Event::getKey);
	}

	private static JoinFunction<Event, ComplexPayload, ComplexPayload> simpleStateUpdate(String strPayload) {
		return (Event first, ComplexPayload second) -> {
			verifyState(strPayload, second);
			return new ComplexPayload(first, strPayload);
		};
	}

	private static JoinFunction<Event, ComplexPayload, ComplexPayload> lastStateUpdate(String strPayload) {
		return (Event first, ComplexPayload second) -> {
			verifyState(strPayload, second);
			boolean isLastEvent = second != null && first.getEventTime() <= second.getEventTime();
			return isLastEvent ? second : new ComplexPayload(first, strPayload);
		};
	}

	private static void verifyState(String strPayload, ComplexPayload state) {
		if (state != null && !state.getStrPayload().equals(strPayload)) {
			System.out.println("State is set or restored incorrectly");
		}
	}
}
