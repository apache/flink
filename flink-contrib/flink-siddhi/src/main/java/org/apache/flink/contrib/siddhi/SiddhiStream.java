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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.contrib.siddhi.operator.SiddhiOperatorContext;
import org.apache.flink.contrib.siddhi.utils.SiddhiOperatorUtils;
import org.apache.flink.contrib.siddhi.utils.SiddhiTypeUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Siddhi CEP API Interface
 */
public abstract class SiddhiStream {
	private final SiddhiCEP environment;
	public SiddhiStream(SiddhiCEP environment){
		this.environment = environment;
	}

	protected SiddhiCEP getEnvironment(){
		return this.environment;
	}

	protected abstract DataStream<Tuple2<String, Object>> toDataStream();

	public static abstract class ExecutableStream extends SiddhiStream {
		public ExecutableStream(SiddhiCEP environment) {
			super(environment);
		}

		public ExecutionSiddhiStream sql(String executionPlan){
			return new ExecutionSiddhiStream(this.toDataStream(),executionPlan,getEnvironment());
		}
	}

	public static class SingleSiddhiStream<T> extends ExecutableStream {
		private final String streamId;
		public SingleSiddhiStream(String streamId,SiddhiCEP environment) {
			super(environment);
			environment.checkStreamDefined(streamId);
			this.streamId = streamId;
		}

		public UnionSiddhiStream<T> union(String streamId, DataStream<T> dataStream, String... fieldNames){
			getEnvironment().registerStream(streamId,dataStream,fieldNames);
			return union(streamId);
		}

		public UnionSiddhiStream<T> union(String ... streamIds){
			Preconditions.checkNotNull(streamIds);
			return new UnionSiddhiStream<T>(this.streamId, Arrays.asList(streamIds),this.getEnvironment());
		}

		@Override
		protected DataStream<Tuple2<String, Object>> toDataStream() {
			final String localStreamId = this.streamId;
			DataStream<T> inputStream = getEnvironment().getDataStream(localStreamId);
			DataStream<Tuple2<String, Object>> outputStream = inputStream.map(new MapFunction<T, Tuple2<String, Object>>() {
				@Override
				public Tuple2<String, Object> map(T value) throws Exception {
					return Tuple2.of(localStreamId,(Object) value);
				}
			});
			if(inputStream instanceof KeyedStream){
				final KeySelector<T, Object> keySelector = ((KeyedStream<T, Object>) inputStream).getKeySelector();
				return outputStream.keyBy(new KeySelector<Tuple2<String,Object>, Object>() {
					@Override
					public Object getKey(Tuple2<String, Object> value) throws Exception {
						return keySelector.getKey((T) value.f1);
					}
				});
			} else {
				return outputStream;
			}
		}
	}

	public static class UnionSiddhiStream<T> extends ExecutableStream{
		private String firstStreamId;
		private List<String> unionStreamIds;

		public UnionSiddhiStream(String firstStreamId,List<String> unionStreamIds,SiddhiCEP environment) {
			super(environment);
			environment.checkStreamDefined(firstStreamId);
			for(String unionStreamId:unionStreamIds){
				environment.checkStreamDefined(unionStreamId);
			}
			this.firstStreamId = firstStreamId;
			this.unionStreamIds = unionStreamIds;
		}

		public UnionSiddhiStream<T> union(String streamId, DataStream<T> dataStream, String... fieldNames){
			getEnvironment().registerStream(streamId,dataStream,fieldNames);
			return union(streamId);
		}

		public UnionSiddhiStream<T> union(String ... streamId){
			List<String> newUnionStreamIds = new LinkedList<>();
			newUnionStreamIds.addAll(unionStreamIds);
			newUnionStreamIds.addAll(Arrays.asList(streamId));
			return new UnionSiddhiStream<T>(this.firstStreamId, newUnionStreamIds,this.getEnvironment());
		}

		@Override
		protected DataStream<Tuple2<String, Object>> toDataStream() {
			final String localFirstStreamId = firstStreamId;
			final List<String> localUnionStreamIds = this.unionStreamIds;
			DataStream<Tuple2<String, Object>> dataStream = getEnvironment().<T>getDataStream(localFirstStreamId).map(new MapFunction<T, Tuple2<String,Object>>() {
				@Override
				public Tuple2<String, Object> map(T value) throws Exception {
					return Tuple2.of(localFirstStreamId,(Object) value);
				}
			});
			for(final String unionStreamId:localUnionStreamIds){
				dataStream = dataStream.union(getEnvironment().<T>getDataStream(unionStreamId).map(new MapFunction<T, Tuple2<String,Object>>() {
					@Override
					public Tuple2<String, Object> map(T value) throws Exception {
						return Tuple2.of(unionStreamId,(Object) value);
					}
				}));
			}
			return dataStream;
		}
	}

	public static class ExecutionSiddhiStream {
		private final SiddhiOperatorContext siddhiContext;
		private final DataStream<Tuple2<String, Object>> dataStream;
		private final SiddhiCEP environment;

		public ExecutionSiddhiStream(DataStream<Tuple2<String, Object>> dataStream, String executionPlan, SiddhiCEP environment) {
			this.siddhiContext = new SiddhiOperatorContext();
			this.siddhiContext.setExecutionPlan(executionPlan);
			this.siddhiContext.setInputStreamSchemas(environment.getDataStreamSchemas());
			this.siddhiContext.setTimeCharacteristic(environment.getExecutionEnvironment().getStreamTimeCharacteristic());
			this.dataStream = dataStream;
			this.environment = environment;
		}

		/**
		 * Return output stream as Tuple
		 */
		public <T extends Tuple> DataStream<T> returns(String outStreamId) {
			return returnsInternal(outStreamId, SiddhiTypeUtils.<T>getTupleTypeInformation(siddhiContext.getFinalExecutionPlan(),outStreamId));
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
			SiddhiOperatorContext context = siddhiContext.copy();
			context.setOutputStreamId(outStreamId);
			context.setOutputStreamType(typeInformation);
			context.setExtensions(environment.getExtensions());
			return SiddhiOperatorUtils.createDataStream(context, this.dataStream);
		}
	}
}
