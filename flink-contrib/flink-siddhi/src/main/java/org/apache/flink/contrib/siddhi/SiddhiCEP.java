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
import org.apache.flink.contrib.siddhi.operator.SiddhiOperatorContext;
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
public class SiddhiCEP {
	private final StreamExecutionEnvironment executionEnvironment;

	private DataStream<Tuple2<String, Object>> delegateStream;
	private final Map<String, DataStream<?>> inputStreams;
	private final Map<String, SiddhiStreamSchema<?>> inputStreamSchemas;

	private final Map<String,Class<?>> extensionRepository = new HashMap<>();

	public SiddhiCEP(StreamExecutionEnvironment streamExecutionEnvironment) {
		this.executionEnvironment = streamExecutionEnvironment;
		this.inputStreams = new HashMap<>();
		this.inputStreamSchemas = new HashMap<>();
	}

	public static <T> SiddhiStream from(String streamId, DataStream<T> inStream, String... fieldNames) {
		SiddhiCEP siddhiCEP = SiddhiCEP.getSiddhiEnvironment(inStream.getExecutionEnvironment());
		return siddhiCEP.define(streamId, inStream, fieldNames);
	}

	public <T> SiddhiStream define(String streamId, DataStream<T> inStream, String... fieldNames){
		this.registerStream(streamId,inStream,fieldNames);
		return new SiddhiStream(this);
	}

	public  <T> void registerStream(final String streamId, DataStream<T> inStream, String... fieldNames) {
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
		this.delegateStream = dataStream;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}

	public void registerExtension(String extensionName, Class<?> extensionClass) {
		if(extensionRepository.containsKey(extensionName)){
			throw new IllegalArgumentException("Extension named "+extensionName+" already registered");
		}
		extensionRepository.put(extensionName,extensionClass);
	}

	public static class SiddhiStream {
		private final SiddhiCEP environment;

		public SiddhiStream(SiddhiCEP environment) {
			this.environment = environment;
		}

		public <T> UnionedStream union(final String streamId, DataStream<T> stream, String... fieldNames){
			environment.registerStream(streamId,stream,fieldNames);
			environment.delegate(this.environment.delegateStream.union(stream.map(new MapFunction<T, Tuple2<String, Object>>() {
				@Override
				public Tuple2<String, Object> map(T value) throws Exception {
					return (Tuple2<String, Object>) Tuple2.of(streamId,value);
				}
			})));
			return new UnionedStream(environment);
		}

		public ExecutedStream sql(String executionPlan) {
			return new ExecutedStream(executionPlan, environment);
		}
	}

	public static class UnionedStream extends SiddhiStream {
		public <T> UnionedStream(SiddhiCEP environment) {
			super(environment);
		}
	}

	public static class ExecutedStream {
		private SiddhiOperatorContext siddhiOperatorContext;
		private SiddhiCEP siddhiCEP;

		public ExecutedStream(String executionPlan, SiddhiCEP environment) {
			siddhiOperatorContext = new SiddhiOperatorContext();
			siddhiOperatorContext.setExecutionPlan(executionPlan);
			siddhiOperatorContext.setInputStreamSchemas(environment.inputStreamSchemas);
			siddhiOperatorContext.setTimeCharacteristic(environment.getExecutionEnvironment().getStreamTimeCharacteristic());
			this.siddhiCEP = environment;
		}

		/**
		 * Return output stream as Tuple
         */
		public <T extends Tuple> DataStream<T> returns(String outStreamId) {
			return returnsInternal(outStreamId, SiddhiTypeUtils.<T>getTupleTypeInformation(siddhiOperatorContext.getFinalExecutionPlan(),outStreamId));
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
			SiddhiOperatorContext context = siddhiOperatorContext.copy();
			context.setOutputStreamId(outStreamId);
			context.setOutputStreamType(typeInformation);
			context.setExtensions(siddhiCEP.extensionRepository);
			return SiddhiOperatorUtils.createDataStream(context, siddhiCEP.delegateStream);
		}
	}

	public static SiddhiCEP getSiddhiEnvironment(StreamExecutionEnvironment streamExecutionEnvironment) {
		return new SiddhiCEP(streamExecutionEnvironment);
	}
}
