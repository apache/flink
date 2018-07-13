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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.state.StateTtlConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.tests.verify.TtlUpdateContext;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.SEQUENCE_GENERATOR_SRC_KEYSPACE;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.SEQUENCE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.SEQUENCE_GENERATOR_SRC_SLEEP_TIME;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

/**
 * A test job for State TTL feature.
 */
public class DataStreamStateTTLTestProgram {
	private static final ConfigOption<Long> STATE_TTL_VERIFIER_TTL_MILLI = ConfigOptions
		.key("state_ttl_verifier.ttl_milli")
		.defaultValue(1000L);

	private static final ConfigOption<Long> STATE_TTL_VERIFIER_PRESICION_MILLI = ConfigOptions
		.key("state_ttl_verifier.precision_milli")
		.defaultValue(5L);

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		setupEnvironment(env, pt);

		int keySpace = pt.getInt(SEQUENCE_GENERATOR_SRC_KEYSPACE.key(), SEQUENCE_GENERATOR_SRC_KEYSPACE.defaultValue());
		int sleepAfterElements = pt.getInt(SEQUENCE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS.key());
		long sleepTime = pt.getLong(SEQUENCE_GENERATOR_SRC_SLEEP_TIME.key());

		Time ttl = Time.milliseconds(pt.getLong(STATE_TTL_VERIFIER_TTL_MILLI.key(), STATE_TTL_VERIFIER_TTL_MILLI.defaultValue()));
		Time precision = Time.milliseconds(pt.getLong(STATE_TTL_VERIFIER_PRESICION_MILLI.key(), STATE_TTL_VERIFIER_PRESICION_MILLI.defaultValue()));

		StateTtlConfiguration ttlConfig = StateTtlConfiguration.newBuilder(ttl).build();

		env
			.addSource(new TtlStateUpdateSource(keySpace, sleepAfterElements, sleepTime))
			.name("TtlStateUpdateSource")
			.keyBy(TtlStateUpdate::getKey)
			.flatMap(new TtlUpdateFunction(ttlConfig))
			.name("TtlUpdateFunction")
			.keyBy(TtlUpdateContext::getKey)
			.flatMap(new TtlVerifyFunction(precision))
			.name("TtlVerificationFunction")
			.addSink(new PrintSinkFunction<>())
			.name("PrintFailedVerifications");

		env.execute("State TTL test job");
	}
}
