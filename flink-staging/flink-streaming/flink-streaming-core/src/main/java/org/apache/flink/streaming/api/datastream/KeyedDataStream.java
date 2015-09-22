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
import org.apache.flink.streaming.runtime.partitioner.HashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

/**
 * A KeyedDataStream represents a {@link DataStream} on which operator state is
 * partitioned by key using a provided {@link KeySelector}. Typical operations supported by a {@link DataStream}
 * are also possible on a KeyedDataStream, with the exception of partitioning methods such as shuffle, forward and groupBy.
 * 
 * 
 * @param <T> The type of the elements in the Keyed Stream
 */
public class KeyedDataStream<T> extends DataStream<T> {
	
	protected final KeySelector<T, ?> keySelector;

	/**
	 * Creates a new {@link KeyedDataStream} using the given {@link KeySelector}
	 * to partition operator state by key.
	 * 
	 * @param dataStream
	 *            Base stream of data
	 * @param keySelector
	 *            Function for determining state partitions
	 */
	public KeyedDataStream(DataStream<T> dataStream, KeySelector<T, ?> keySelector) {
		super(dataStream.getExecutionEnvironment(), new PartitionTransformation<T>(dataStream.getTransformation(), new HashPartitioner<T>(keySelector)));
		this.keySelector = keySelector;
	}

	public KeySelector<T, ?> getKeySelector() {
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

		((OneInputTransformation<T, R>) returnStream.getTransformation()).setStateKeySelector(
				keySelector);
		return returnStream;
	}

	@Override
	public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {
		DataStreamSink<T> result = super.addSink(sinkFunction);
		result.getTransformation().setStateKeySelector(keySelector);
		return result;
	}
}
