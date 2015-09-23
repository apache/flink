/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.windowing.windowpolicy.WindowPolicy;
import org.apache.flink.streaming.runtime.partitioner.HashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

/**
 * A KeyedDataStream represents a {@link DataStream} on which operator state is
 * partitioned by key using a provided {@link KeySelector}. Typical operations supported by a {@link DataStream}
 * are also possible on a KeyedDataStream, with the exception of partitioning methods such as shuffle, forward and groupBy.
 * 
 * 
 * @param <T> The type of the elements in the Keyed Stream.
 * @param <K> The type of the key in the Keyed Stream.
 */
public class KeyedDataStream<T, K> extends DataStream<T> {
	
	protected final KeySelector<T, K> keySelector;

	/**
	 * Creates a new {@link KeyedDataStream} using the given {@link KeySelector}
	 * to partition operator state by key.
	 * 
	 * @param dataStream
	 *            Base stream of data
	 * @param keySelector
	 *            Function for determining state partitions
	 */
	public KeyedDataStream(DataStream<T> dataStream, KeySelector<T, K> keySelector) {
		super(dataStream.getExecutionEnvironment(), new PartitionTransformation<T>(dataStream.getTransformation(), new HashPartitioner<T>(keySelector)));
		this.keySelector = keySelector;
	}

	
	public KeySelector<T, K> getKeySelector() {
		return this.keySelector;
	}

	
	@Override
	protected DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
		throw new UnsupportedOperationException("Cannot override partitioning for KeyedDataStream.");
	}

	
	@Override
	public <R> SingleOutputStreamOperator<R, ?> transform(String operatorName,
			TypeInformation<R> outTypeInfo, OneInputStreamOperator<T, R> operator) {

		SingleOutputStreamOperator<R, ?> returnStream = super.transform(operatorName, outTypeInfo,operator);

		((OneInputTransformation<T, R>) returnStream.getTransformation()).setStateKeySelector(keySelector);
		return returnStream;
	}

	
	
	@Override
	public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {
		DataStreamSink<T> result = super.addSink(sinkFunction);
		result.getTransformation().setStateKeySelector(keySelector);
		return result;
	}
	
	// ------------------------------------------------------------------------
	//  Windowing
	// ------------------------------------------------------------------------

	/**
	 * Windows this data stream to a KeyedWindowDataStream, which evaluates windows over a key
	 * grouped stream. The window is defined by a single policy.
	 * <p>
	 * For time windows, these single-policy windows result in tumbling time windows.
	 *     
	 * @param policy The policy that defines the window.
	 * @return The windows data stream. 
	 */
	public KeyedWindowDataStream<T, K> window(WindowPolicy policy) {
		return new KeyedWindowDataStream<T, K>(this, policy);
	}

	/**
	 * Windows this data stream to a KeyedWindowDataStream, which evaluates windows over a key
	 * grouped stream. The window is defined by a window policy, plus a slide policy.
	 * <p>
	 * For time windows, these slide policy windows result in sliding time windows.
	 * 
	 * @param window The policy that defines the window.
	 * @param slide The additional policy defining the slide of the window. 
	 * @return The windows data stream.
	 */
	public KeyedWindowDataStream<T, K> window(WindowPolicy window, WindowPolicy slide) {
		return new KeyedWindowDataStream<T, K>(this, window, slide);
	}
}
