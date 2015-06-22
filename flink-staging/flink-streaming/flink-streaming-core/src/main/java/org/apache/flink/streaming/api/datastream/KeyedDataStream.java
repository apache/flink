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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

/**
 * A KeyedDataStream represents a {@link DataStream} on which operator state is
 * partitioned by key using a provided {@link KeySelector}. Typical operations supported by a {@link DataStream}
 * are also possible on a KeyedDataStream, with the exception of partitioning methods such as shuffle, forward and groupBy.
 * 
 * 
 * @param <OUT>
 *            The output type of the {@link KeyedDataStream}.
 */
public class KeyedDataStream<OUT> extends DataStream<OUT> {
	KeySelector<OUT, ?> keySelector;

	/**
	 * Creates a new {@link KeyedDataStream} using the given {@link KeySelector}
	 * to partition operator state by key.
	 * 
	 * @param dataStream
	 *            Base stream of data
	 * @param keySelector
	 *            Function for determining state partitions
	 */
	public KeyedDataStream(DataStream<OUT> dataStream, KeySelector<OUT, ?> keySelector) {
		super(dataStream.partitionByHash(keySelector));
		this.keySelector = keySelector;
	}

	protected KeyedDataStream(KeyedDataStream<OUT> dataStream) {
		super(dataStream);
		this.keySelector = dataStream.keySelector;
	}

	public KeySelector<OUT, ?> getKeySelector() {
		return this.keySelector;
	}

	@Override
	protected DataStream<OUT> setConnectionType(StreamPartitioner<OUT> partitioner) {
		throw new UnsupportedOperationException("Cannot override partitioning for KeyedDataStream.");
	}

	@Override
	public KeyedDataStream<OUT> copy() {
		return new KeyedDataStream<OUT>(this);
	}

	@Override
	public <R> SingleOutputStreamOperator<R, ?> transform(String operatorName,
			TypeInformation<R> outTypeInfo, OneInputStreamOperator<OUT, R> operator) {
		SingleOutputStreamOperator<R, ?> returnStream = super.transform(operatorName, outTypeInfo,operator);
		streamGraph.setKey(returnStream.getId(), keySelector);
		return returnStream;
	}
}
