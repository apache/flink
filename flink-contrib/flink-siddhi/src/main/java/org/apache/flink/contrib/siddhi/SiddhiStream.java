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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.contrib.siddhi.operator.SiddhiOperatorInformation;
import org.apache.flink.contrib.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.contrib.siddhi.utils.SiddhiTypeUtils;
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

	private DataStream<Tuple2<String, Object>> previousStream;
	private DataStream<Tuple2<String, Object>> delegateStream;
	private final Map<String, DataStream<?>> inputStreams;
	private final Map<String, SiddhiStreamSchema<?>> inputStreamSchemas;

	public SiddhiStream(StreamExecutionEnvironment streamExecutionEnvironment) {
		this.executionEnvironment = streamExecutionEnvironment;
		this.inputStreams = new HashMap<>();
		this.inputStreamSchemas = new HashMap<>();
	}

	public static <T> DefinedStream from(String streamId, DataStream<T> inStream, String... fieldNames) {
		SiddhiStream siddhiStream = SiddhiStream.newStream(inStream.getExecutionEnvironment());
		return siddhiStream.define(streamId, inStream, fieldNames);
	}

	public <T> DefinedStream define(String streamId, DataStream<T> inStream, String... fieldNames){
		this.register(streamId,inStream,fieldNames);
		return new DefinedStream(this);
	}

	public <T> void register(final String streamId, DataStream<T> inStream, String... fieldNames) {
		if(inputStreams.isEmpty()){
			delegateStream = inStream.map(new MapFunction<T, Tuple2<String, Object>>() {
				@Override
				public Tuple2<String, Object> map(Object value) throws Exception {
					return Tuple2.of(streamId,value);
				}
			});
		} else if (inputStreams.containsKey(streamId)) {
			throw new IllegalArgumentException("Input stream: " + streamId + " already exists");
		}
		inputStreams.put(streamId, inStream);
		SiddhiStreamSchema<T> schema = new SiddhiStreamSchema<>(inStream.getType(), fieldNames);
		schema.setTypeSerializer(schema.getTypeInfo().createSerializer(inStream.getExecutionConfig()));
		inputStreamSchemas.put(streamId, schema);
	}

	private void delegate(DataStream<Tuple2<String, Object>> dataStream){
		this.previousStream = delegateStream;
		this.delegateStream = dataStream;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}

	public static class DefinedStream {
		private final SiddhiStream environment;

		public DefinedStream(SiddhiStream environment) {
			this.environment = environment;
		}

		public <T> UnionedStream union(final String streamId, DataStream<T> stream, String... fieldNames){
			environment.register(streamId,stream,fieldNames);
			environment.delegate(this.environment.delegateStream.union(stream.map(new MapFunction<T, Tuple2<String, Object>>() {
				@Override
				public Tuple2<String, Object> map(T value) throws Exception {
					return (Tuple2<String, Object>) Tuple2.of(streamId,value);
				}
			})));
			return new UnionedStream(environment);
		}

		public ExecutedStream query(String executionPlan) {
			return new ExecutedStream(executionPlan, environment);
		}
	}

	public static class UnionedStream extends DefinedStream {
		public <T> UnionedStream(SiddhiStream environment) {
			super(environment);
		}
	}

	public static class ExecutedStream {
		private SiddhiOperatorInformation siddhiOperatorInformation;
		private SiddhiStream siddhiStream;

		public ExecutedStream(String executionPlan, SiddhiStream environment) {
			siddhiOperatorInformation = new SiddhiOperatorInformation();
			siddhiOperatorInformation.setExecutionPlan(executionPlan);
			siddhiOperatorInformation.setInputStreamSchemas(environment.inputStreamSchemas);
			siddhiOperatorInformation.setTimeCharacteristic(environment.getExecutionEnvironment().getStreamTimeCharacteristic());
			this.siddhiStream = environment;
		}

		/**
		 * Return output stream as Tuple
         */
		public <T extends Tuple> DataStream<T> returns(String outStreamId) {
			return returnsInternal(outStreamId, SiddhiTypeUtils.<T>getTupleTypeInformation(siddhiOperatorInformation.getFinalExecutionPlan(),outStreamId));
		}

		/**
		 * Return output stream as Map[String,Object]
         */
		public DataStream<Map> returnAsMap(String outStreamId) {
			return this.returnsInternal(outStreamId,SiddhiTypeUtils.getMapTypeInformation());
		}

		/**
		 * Return output stream as POJO class.
         */
		public <T> DataStream<T> returns(String outStreamId, Class<T> outType) {
			TypeInformation<T> typeInformation = TypeExtractor.getForClass(outType);
			return returnsInternal(outStreamId,typeInformation);
		}

		private  <T> DataStream<T> returnsInternal(String outStreamId, TypeInformation<T> typeInformation) {
			SiddhiOperatorInformation context = siddhiOperatorInformation.copy();
			context.setOutputStreamId(outStreamId);
			context.setOutputStreamType(typeInformation);
			return SiddhiOperatorUtils.createDataStream(context, siddhiStream.delegateStream);
		}
	}

	public static SiddhiStream newStream(StreamExecutionEnvironment streamExecutionEnvironment) {
		return new SiddhiStream(streamExecutionEnvironment);
	}
}
