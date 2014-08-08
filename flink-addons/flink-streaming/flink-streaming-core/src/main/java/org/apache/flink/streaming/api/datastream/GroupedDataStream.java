/**
 *
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
 *
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.operator.BatchGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.GroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowGroupReduceInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;

/**
 * A GroupedDataStream represents a data stream which has been partitioned by
 * the given key in the values. Operators like {@link #reduce},
 * {@link #batchReduce} etc. can be applied on the {@link GroupedDataStream}.
 *
 * @param <OUT>
 *            The output type of the {@link GroupedDataStream}.
 */
public class GroupedDataStream<OUT> {

	DataStream<OUT> dataStream;
	int keyPosition;

	protected GroupedDataStream(DataStream<OUT> dataStream, int keyPosition) {
		this.dataStream = dataStream.copy();
		this.keyPosition = keyPosition;
	}

	/**
	 * Applies a reduce transformation on the grouped data stream grouped on by
	 * the given key position. The {@link ReduceFunction} will receive input
	 * values based on the key value. Only input values with the same key will
	 * go to the same reducer.The user can also extend
	 * {@link RichReduceFunction} to gain access to other features provided by
	 * the {@link RichFuntion} interface.
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values with the same key.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reducer) {
		return dataStream.addFunction("groupReduce", reducer,
				new FunctionTypeWrapper<OUT, Tuple, OUT>(reducer, ReduceFunction.class, 0, -1, 0),
				new GroupReduceInvokable<OUT>(reducer, keyPosition)).partitionBy(keyPosition);
	}

	/**
	 * Applies a group reduce transformation on preset chunks of the grouped
	 * data stream. The {@link ReduceFunction} will receive input values based
	 * on the key value. Only input values with the same key will go to the same
	 * reducer.When the reducer has ran for all the values in the batch, the
	 * batch is slid forward. access to other features provided by the
	 * {@link RichFuntion} interface.
	 * 
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values with the same key.
	 * @param batchSize
	 *            The size of the data stream chunk (the number of values in the
	 *            batch).
	 * @return The transformed {@link DataStream}.
	 */
	public SingleOutputStreamOperator<OUT, ?> batchReduce(ReduceFunction<OUT> reducer, int batchSize) {
		return batchReduce(reducer, batchSize, batchSize);
	}

	/**
	 * Applies a group reduce transformation on preset chunks of the grouped
	 * data stream in a sliding window fashion. The {@link ReduceFunction} will
	 * receive input values based on the key value. Only input values with the
	 * same key will go to the same reducer. When the reducer has ran for all
	 * the values in the batch, the batch is slid forward. The user can also
	 * extend {@link RichReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values with the same key.
	 * @param batchSize
	 *            The size of the data stream chunk (the number of values in the
	 *            batch).
	 * @param slideSize
	 *            The number of values the batch is slid by.
	 * @return The transformed {@link DataStream}.
	 */
	public SingleOutputStreamOperator<OUT, ?> batchReduce(ReduceFunction<OUT> reducer,
			long batchSize, long slideSize) {
		return dataStream.addFunction("batchReduce", reducer,
				new FunctionTypeWrapper<OUT, Tuple, OUT>(reducer, ReduceFunction.class, 0, -1,
						0), new BatchGroupReduceInvokable<OUT>(reducer, batchSize, slideSize,
						keyPosition));
	}

	/**
	 * Applies a group reduce transformation on preset "time" chunks of the
	 * grouped data stream. The {@link ReduceFunction} will receive input values
	 * based on the key value. Only input values with the same key will go to
	 * the same reducer.When the reducer has ran for all the values in the
	 * batch, the window is shifted forward gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each time window.
	 * @param windowSize
	 *            SingleOutputStreamOperator The time window to run the reducer
	 *            on, in milliseconds.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> windowReduce(ReduceFunction<OUT> reducer,
			long windowSize) {
		return windowReduce(reducer, windowSize, windowSize, windowSize);
	}

	/**
	 * Applies a group reduce transformation on preset "time" chunks of the
	 * grouped data stream in a sliding window fashion. The
	 * {@link ReduceFunction} will receive input values based on the key value.
	 * Only input values with the same key will go to the same reducer. When the
	 * reducer has ran for all the values in the batch, the window is shifted
	 * forward. The user can also extend {@link RichReduceFunction} to gain
	 * access to other features provided by the {@link RichFuntion} interface.
	 *
	 * @param reducer
	 *            The GroupReduceFunction that is called for each time window.
	 * @param windowSize
	 *            SingleOutputStreamOperator The time window to run the reducer
	 *            on, in milliseconds.
	 * @param slideSize
	 *            The time interval the batch is slid by.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> windowReduce(ReduceFunction<OUT> reducer,
			long windowSize, long slideInterval, long timeUnitInMillis) {
		return dataStream.addFunction("batchReduce", reducer,
				new FunctionTypeWrapper<OUT, Tuple, OUT>(reducer, ReduceFunction.class, 0, -1,
						0), new WindowGroupReduceInvokable<OUT>(reducer, windowSize, slideInterval,
						keyPosition));
	}

}
