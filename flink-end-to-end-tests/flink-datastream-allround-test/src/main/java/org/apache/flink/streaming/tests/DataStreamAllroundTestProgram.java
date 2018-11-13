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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.tests.artificialstate.ComplexPayload;

import java.util.Collections;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createArtificialKeyedStateMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createArtificialOperatorStateMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createEventSource;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createFailureMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createSemanticsCheckMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createTimestampExtractor;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.isSimulateFailures;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

/**
 * A general purpose test job for Flink's DataStream API operators and primitives.
 *
 * <p>The job is constructed of generic components from {@link DataStreamAllroundTestJobFactory}.
 * It currently covers the following aspects that are frequently present in Flink DataStream jobs:
 * <ul>
 *     <li>A generic Kryo input type.</li>
 *     <li>A state type for which we register a {@link KryoSerializer}.</li>
 *     <li>Operators with {@link ValueState}.</li>
 *     <li>Operators with union state.</li>
 *     <li>Operators with broadcast state.</li>
 * </ul>
 *
 * <p>The cli job configuration options are described in {@link DataStreamAllroundTestJobFactory}.
 *
 */
public class DataStreamAllroundTestProgram {
	private static final String KEYED_STATE_OPER_NAME = "ArtificalKeyedStateMapper";
	private static final String OPERATOR_STATE_OPER_NAME = "ArtificalOperatorStateMapper";
	private static final String SEMANTICS_CHECK_MAPPER_NAME = "SemanticsCheckMapper";
	private static final String FAILURE_MAPPER_NAME = "FailureMapper";

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		setupEnvironment(env, pt);

		DataStream<Event> eventStream = env.addSource(createEventSource(pt)).uid("0001")
			.assignTimestampsAndWatermarks(createTimestampExtractor(pt))
			.keyBy(Event::getKey)
			.map(createArtificialKeyedStateMapper(
					// map function simply forwards the inputs
					(MapFunction<Event, Event>) in -> in,
					// state is verified and updated per event as a wrapped ComplexPayload state object
					(Event first, ComplexPayload second) -> {
							if (second != null && !second.getStrPayload().equals(KEYED_STATE_OPER_NAME)) {
								System.out.println("State is set or restored incorrectly");
							}
							return new ComplexPayload(first, KEYED_STATE_OPER_NAME);
						},
					Collections.singletonList(
						new KryoSerializer<>(ComplexPayload.class, env.getConfig()))
				)
			)
			.name(KEYED_STATE_OPER_NAME)
			.returns(Event.class)
			.uid("0002");

		DataStream<Event> eventStream2 = eventStream
			.map(createArtificialOperatorStateMapper((MapFunction<Event, Event>) in -> in))
			.returns(Event.class)
			.name(OPERATOR_STATE_OPER_NAME).uid("0003");

		if (isSimulateFailures(pt)) {
			eventStream2 = eventStream2
				.map(createFailureMapper(pt))
				.setParallelism(1)
				.name(FAILURE_MAPPER_NAME).uid("0004");
		}

		eventStream2.keyBy(Event::getKey)
			.flatMap(createSemanticsCheckMapper(pt))
			.name(SEMANTICS_CHECK_MAPPER_NAME).uid("0005")
			.addSink(new PrintSinkFunction<>()).uid("0006");

		env.execute("General purpose test job");
	}
}
