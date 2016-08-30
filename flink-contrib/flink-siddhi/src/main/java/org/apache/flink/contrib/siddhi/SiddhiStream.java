/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.contrib.siddhi.operator.SiddhiOperatorContext;
import org.apache.flink.contrib.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.contrib.siddhi.utils.SiddhiOperatorUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Siddhi CEP Environment Context
 */
@Public
public class SiddhiStream {
	private final StreamExecutionEnvironment executionEnvironment;

	private final Map<String, DataStream<?>> inputStreams;
	private final Map<String, SiddhiStreamSchema<?>> inputStreamSchemas;

	public SiddhiStream(StreamExecutionEnvironment streamExecutionEnvironment) {
		this.executionEnvironment = streamExecutionEnvironment;
		this.inputStreams = new HashMap<>();
		this.inputStreamSchemas = new HashMap<>();
	}

	public static <T> WithExecutionEnvironment inject(String streamId, DataStream<T> inStream, String... fieldNames) {
		SiddhiStream siddhiStream = SiddhiStream.newSiddhiStream(inStream.getExecutionEnvironment());
		return new WithExecutionEnvironment(siddhiStream).with(streamId, inStream, fieldNames);
	}

	public <T> void register(String streamId, DataStream<T> inStream, String... fieldNames) {
		if (inputStreams.containsKey(streamId)) {
			throw new IllegalArgumentException("Input stream: " + streamId + " already exists");
		}
		inputStreams.put(streamId, inStream);
		SiddhiStreamSchema<T> schema = new SiddhiStreamSchema<>(inStream.getType(), fieldNames);
		schema.setTypeSerializer(schema.getTypeInfo().createSerializer(inStream.getExecutionConfig()));
		inputStreamSchemas.put(streamId, schema);
	}

	public WithExecutionPlan apply(String executionPlan) {
		return new WithExecutionEnvironment(this).apply(executionPlan);
	}

	public Map<String, DataStream<?>> getInputStreams() {
		return this.inputStreams;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}

	public static class WithExecutionEnvironment {
		private final SiddhiStream environment;

		public WithExecutionEnvironment(SiddhiStream environment) {
			this.environment = environment;
		}

		public <T> WithExecutionEnvironment with(String streamId, DataStream<T> inStream, String... fieldNames) {
			environment.register(streamId, inStream, fieldNames);
			return this;
		}

		public WithExecutionPlan apply(String executionPlan) {
			return new WithExecutionPlan(executionPlan, environment);
		}

		public SiddhiStream environment() {
			return this.environment;
		}
	}

	public static class WithExecutionPlan {
		private SiddhiOperatorContext siddhiOperatorContext;
		private SiddhiStream siddhiStream;

		public WithExecutionPlan(String executionPlan, SiddhiStream environment) {
			siddhiOperatorContext = new SiddhiOperatorContext();
			siddhiOperatorContext.setExecutionExpression(executionPlan);
			siddhiOperatorContext.setInputStreamSchemas(environment.inputStreamSchemas);
			siddhiOperatorContext.setTimeCharacteristic(environment.getExecutionEnvironment().getStreamTimeCharacteristic());
			this.siddhiStream = environment;
		}

		public DataStream<Map<String, Object>> returns(String outStreamId) {
			siddhiOperatorContext.setOutputStreamId(outStreamId);
			siddhiOperatorContext.setOutputStreamType(TypeExtractor.createTypeInfo(Map.class));
			return SiddhiOperatorUtils.createDataStream(siddhiOperatorContext, siddhiStream);
		}

		public <T> DataStream<T> returns(String outStreamId, Class<T> outType) {
			siddhiOperatorContext.setOutputStreamId(outStreamId);
			siddhiOperatorContext.setOutputStreamType(TypeExtractor.createTypeInfo(outType));
			return SiddhiOperatorUtils.createDataStream(siddhiOperatorContext, siddhiStream);
		}
	}

	public static SiddhiStream newSiddhiStream(StreamExecutionEnvironment streamExecutionEnvironment) {
		return new SiddhiStream(streamExecutionEnvironment);
	}
}
