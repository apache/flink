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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.BatchGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.BatchReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.GroupedWindowGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.GroupedWindowReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowReduceInvokable;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;

/**
 * A {@link WindowDataStream} represents a data stream whose elements are
 * batched together in a sliding window. operations like
 * {@link #reduce(ReduceFunction)} or {@link #reduceGroup(GroupReduceFunction)}
 * are applied for each window and the window is slid afterwards.
 *
 * @param <OUT>
 *            The output type of the {@link WindowDataStream}
 */
public class WindowDataStream<OUT> extends BatchedDataStream<OUT> {

	TimeStamp<OUT> timeStamp;

	protected WindowDataStream(DataStream<OUT> dataStream, long windowSize, long slideInterval,
			TimeStamp<OUT> timeStamp) {
		super(dataStream, windowSize, slideInterval);
		this.timeStamp = timeStamp;
	}

	protected WindowDataStream(WindowDataStream<OUT> windowDataStream) {
		super(windowDataStream);
		this.timeStamp = windowDataStream.timeStamp;
	}

	public WindowDataStream<OUT> groupBy(int keyPosition) {
		return new WindowDataStream<OUT>(dataStream.groupBy(keyPosition), batchSize, slideSize,
				timeStamp);
	}

	protected BatchReduceInvokable<OUT> getReduceInvokable(ReduceFunction<OUT> reducer) {
		WindowReduceInvokable<OUT> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowReduceInvokable<OUT>(reducer, batchSize, slideSize,
					keyPosition, timeStamp);
		} else {
			invokable = new WindowReduceInvokable<OUT>(reducer, batchSize, slideSize, timeStamp);
		}
		return invokable;
	}

	protected <R> BatchGroupReduceInvokable<OUT, R> getGroupReduceInvokable(
			GroupReduceFunction<OUT, R> reducer) {
		BatchGroupReduceInvokable<OUT, R> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowGroupReduceInvokable<OUT, R>(reducer, batchSize,
					slideSize, keyPosition, timeStamp);
		} else {
			invokable = new WindowGroupReduceInvokable<OUT, R>(reducer, batchSize, slideSize,
					timeStamp);
		}
		return invokable;
	}

	public WindowDataStream<OUT> copy() {
		return new WindowDataStream<OUT>(this);
	}

}
