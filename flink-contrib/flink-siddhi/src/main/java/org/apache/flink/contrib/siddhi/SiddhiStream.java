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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.contrib.siddhi.operator.SiddhiOperatorContext;
import org.apache.flink.contrib.siddhi.utils.SiddhiStreamFactory;
import org.apache.flink.contrib.siddhi.utils.SiddhiTypeFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Siddhi CEP Stream API
 */
@PublicEvolving
public abstract class SiddhiStream {
	private final SiddhiCEP cepEnvironment;

	/**
	 * @param cepEnvironment SiddhiCEP cepEnvironment.
     */
	public SiddhiStream(SiddhiCEP cepEnvironment) {
		Preconditions.checkNotNull(cepEnvironment,"SiddhiCEP cepEnvironment is null");
		this.cepEnvironment = cepEnvironment;
	}

	/**
	 * @return current SiddhiCEP cepEnvironment.
     */
	protected SiddhiCEP getCepEnvironment() {
		return this.cepEnvironment;
	}

	/**
	 * @return Transform SiddhiStream to physical DataStream
     */
	protected abstract DataStream<Tuple2<String, Object>> toDataStream();

	/**
	 * Convert DataStream&lt;T&gt; to DataStream&lt;Tuple2&lt;String,T&gt;&gt;.
	 * If it's KeyedStream. pass through original keySelector
	 */
	protected <T> DataStream<Tuple2<String, Object>> convertDataStream(DataStream<T> dataStream, String streamId) {
		final String streamIdInClosure = streamId;
		DataStream<Tuple2<String, Object>> resultStream = dataStream.map(new MapFunction<T, Tuple2<String, Object>>() {
			@Override
			public Tuple2<String, Object> map(T value) throws Exception {
				return Tuple2.of(streamIdInClosure, (Object) value);
			}
		});
		if (dataStream instanceof KeyedStream) {
			final KeySelector<T, Object> keySelector = ((KeyedStream<T, Object>) dataStream).getKeySelector();
			final KeySelector<Tuple2<String, Object>, Object> keySelectorInClosure = new KeySelector<Tuple2<String, Object>, Object>() {
				@Override
				public Object getKey(Tuple2<String, Object> value) throws Exception {
					return keySelector.getKey((T) value.f1);
				}
			};
			return resultStream.keyBy(keySelectorInClosure);
		} else {
			return resultStream;
		}
	}

	/**
	 * ExecutableStream context to define execution logic, i.e. SiddhiCEP execution plan.
     */
	public static abstract class ExecutableStream extends SiddhiStream {
		public ExecutableStream(SiddhiCEP environment) {
			super(environment);
		}

		/**
		 * @param executionPlan Siddhi SQL-Like execution plan query
         * @return ExecutionSiddhiStream context
         */
		public ExecutionSiddhiStream sql(String executionPlan) {
			Preconditions.checkNotNull(executionPlan,"executionPlan");
			return new ExecutionSiddhiStream(this.toDataStream(), executionPlan, getCepEnvironment());
		}
	}

	/**
	 * Initial Single Siddhi Stream Context
     */
	public static class SingleSiddhiStream<T> extends ExecutableStream {
		private final String streamId;

		public SingleSiddhiStream(String streamId, SiddhiCEP environment) {
			super(environment);
			environment.checkStreamDefined(streamId);
			this.streamId = streamId;
		}


		/**
		 * Define siddhi stream with streamId, source <code>DataStream</code> and stream schema and as the first stream of {@link UnionSiddhiStream}
		 *
		 * @param streamId Unique siddhi streamId
		 * @param dataStream DataStream to bind to the siddhi stream.
		 * @param fieldNames Siddhi stream schema field names
		 *
		 * @return {@link UnionSiddhiStream} context
		 */
		public UnionSiddhiStream<T> union(String streamId, DataStream<T> dataStream, String... fieldNames) {
			getCepEnvironment().registerStream(streamId, dataStream, fieldNames);
			return union(streamId);
		}

		/**
		 * @param streamIds Defined siddhi streamIds to union
         * @return {@link UnionSiddhiStream} context
         */
		public UnionSiddhiStream<T> union(String... streamIds) {
			Preconditions.checkNotNull(streamIds,"streamIds");
			return new UnionSiddhiStream<T>(this.streamId, Arrays.asList(streamIds), this.getCepEnvironment());
		}

		@Override
		protected DataStream<Tuple2<String, Object>> toDataStream() {
			return convertDataStream(getCepEnvironment().getDataStream(this.streamId), this.streamId);
		}
	}

	public static class UnionSiddhiStream<T> extends ExecutableStream {
		private String firstStreamId;
		private List<String> unionStreamIds;

		public UnionSiddhiStream(String firstStreamId, List<String> unionStreamIds, SiddhiCEP environment) {
			super(environment);
			Preconditions.checkNotNull(firstStreamId,"firstStreamId");
			Preconditions.checkNotNull(unionStreamIds,"unionStreamIds");
			environment.checkStreamDefined(firstStreamId);
			for (String unionStreamId : unionStreamIds) {
				environment.checkStreamDefined(unionStreamId);
			}
			this.firstStreamId = firstStreamId;
			this.unionStreamIds = unionStreamIds;
		}

		/**
		 * Define siddhi stream with streamId, source <code>DataStream</code> and stream schema and continue to union it with current stream.
		 *
		 * @param streamId Unique siddhi streamId
		 * @param dataStream DataStream to bind to the siddhi stream.
		 * @param fieldNames Siddhi stream schema field names
		 *
		 * @return {@link UnionSiddhiStream} context
		 */
		public UnionSiddhiStream<T> union(String streamId, DataStream<T> dataStream, String... fieldNames) {
			Preconditions.checkNotNull(streamId,"streamId");
			Preconditions.checkNotNull(dataStream,"dataStream");
			Preconditions.checkNotNull(fieldNames,"fieldNames");
			getCepEnvironment().registerStream(streamId, dataStream, fieldNames);
			return union(streamId);
		}

		/**
		 * @param streamId another defined streamId to union with.
		 * @return {@link UnionSiddhiStream} context
         */
		public UnionSiddhiStream<T> union(String... streamId) {
			List<String> newUnionStreamIds = new LinkedList<>();
			newUnionStreamIds.addAll(unionStreamIds);
			newUnionStreamIds.addAll(Arrays.asList(streamId));
			return new UnionSiddhiStream<T>(this.firstStreamId, newUnionStreamIds, this.getCepEnvironment());
		}

		@Override
		protected DataStream<Tuple2<String, Object>> toDataStream() {
			final String localFirstStreamId = firstStreamId;
			final List<String> localUnionStreamIds = this.unionStreamIds;
			DataStream<Tuple2<String, Object>> dataStream = convertDataStream(getCepEnvironment().<T>getDataStream(localFirstStreamId), this.firstStreamId);
			for (String unionStreamId : localUnionStreamIds) {
				dataStream = dataStream.union(convertDataStream(getCepEnvironment().<T>getDataStream(unionStreamId), unionStreamId));
			}
			return dataStream;
		}
	}

	public static class ExecutionSiddhiStream {
		private final DataStream<Tuple2<String, Object>> dataStream;
		private final SiddhiCEP environment;
		private final String executionPlan;

		public ExecutionSiddhiStream(DataStream<Tuple2<String, Object>> dataStream, String executionPlan, SiddhiCEP environment) {
			this.executionPlan = executionPlan;
			this.dataStream = dataStream;
			this.environment = environment;
		}

		/**
		 * @param outStreamId The <code>streamId</code> to return as data stream.
		 * @param <T>         Type information should match with stream definition.
		 *                    During execution phase, it will automatically build type information based on stream definition.
		 * @return Return output stream as Tuple
		 * @see SiddhiTypeFactory
		 */
		public <T extends Tuple> DataStream<T> returns(String outStreamId) {
			SiddhiOperatorContext siddhiContext = new SiddhiOperatorContext();
			siddhiContext.setExecutionPlan(executionPlan);
			siddhiContext.setInputStreamSchemas(environment.getDataStreamSchemas());
			siddhiContext.setTimeCharacteristic(environment.getExecutionEnvironment().getStreamTimeCharacteristic());
			siddhiContext.setOutputStreamId(outStreamId);
			siddhiContext.setExtensions(environment.getExtensions());
			siddhiContext.setExecutionConfig(environment.getExecutionEnvironment().getConfig());
			TypeInformation<T> typeInformation =
				SiddhiTypeFactory.getTupleTypeInformation(siddhiContext.getFinalExecutionPlan(), outStreamId);
			siddhiContext.setOutputStreamType(typeInformation);
			return returnsInternal(siddhiContext);
		}

		/**
		 * @return Return output stream as <code>DataStream&lt;Map&lt;String,Object&gt;&gt;</code>,
		 * out type is <code>LinkedHashMap&lt;String,Object&gt;</code> and guarantee field order
		 * as defined in siddhi execution plan
		 * @see java.util.LinkedHashMap
		 */
		public DataStream<Map<String, Object>> returnAsMap(String outStreamId) {
			return this.returnsInternal(outStreamId, SiddhiTypeFactory.getMapTypeInformation());
		}

		/**
		 * @param outStreamId OutStreamId
		 * @param outType     Output type class
		 * @param <T>         Output type
		 * @return Return output stream as POJO class.
		 */
		public <T> DataStream<T> returns(String outStreamId, Class<T> outType) {
			TypeInformation<T> typeInformation = TypeExtractor.getForClass(outType);
			return returnsInternal(outStreamId, typeInformation);
		}

		private <T> DataStream<T> returnsInternal(String outStreamId, TypeInformation<T> typeInformation) {
			SiddhiOperatorContext siddhiContext = new SiddhiOperatorContext();
			siddhiContext.setExecutionPlan(executionPlan);
			siddhiContext.setInputStreamSchemas(environment.getDataStreamSchemas());
			siddhiContext.setTimeCharacteristic(environment.getExecutionEnvironment().getStreamTimeCharacteristic());
			siddhiContext.setOutputStreamId(outStreamId);
			siddhiContext.setOutputStreamType(typeInformation);
			siddhiContext.setExtensions(environment.getExtensions());
			siddhiContext.setExecutionConfig(environment.getExecutionEnvironment().getConfig());
			return returnsInternal(siddhiContext);
		}

		private <T> DataStream<T> returnsInternal(SiddhiOperatorContext siddhiContext) {
			return SiddhiStreamFactory.createDataStream(siddhiContext, this.dataStream);
		}
	}
}
