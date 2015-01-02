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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedWindowReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoWindowReduceInvokable;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

/**
 * A {@link CoWindowDataStream} represents two data streams whose elements are
 * batched together into sliding windows. Operation
 * {@link #reduce(CoReduceFunction)} can be applied for each window.
 * 
 * @param <IN1>
 *            The type of the first input data stream
 * @param <IN2>
 *            The type of the second input data stream
 */
public class CoWindowDataStream<IN1, IN2> extends CoBatchedDataStream<IN1, IN2> {
	TimestampWrapper<IN1> timeStamp1;
	TimestampWrapper<IN2> timeStamp2;

	protected CoWindowDataStream(DataStream<IN1> dataStream1, DataStream<IN2> dataStream2,
			long windowSize1, long windowSize2, long slideInterval1, long slideInterval2,
			TimestampWrapper<IN1> timeStamp1, TimestampWrapper<IN2> timeStamp2) {
		super(dataStream1, dataStream2, windowSize1, windowSize2, slideInterval1, slideInterval2);
		this.timeStamp1 = timeStamp1;
		this.timeStamp2 = timeStamp2;
	}

	protected CoWindowDataStream(ConnectedDataStream<IN1, IN2> coDataStream, long windowSize1,
			long windowSize2, long slideInterval1, long slideInterval2,
			TimestampWrapper<IN1> timeStamp1, TimestampWrapper<IN2> timeStamp2) {
		super(coDataStream, windowSize1, windowSize2, slideInterval1, slideInterval2);
		this.timeStamp1 = timeStamp1;
		this.timeStamp2 = timeStamp2;
	}

	protected CoWindowDataStream(CoWindowDataStream<IN1, IN2> coWindowDataStream) {
		super(coWindowDataStream);
		this.timeStamp1 = coWindowDataStream.timeStamp1;
		this.timeStamp2 = coWindowDataStream.timeStamp2;
	}

	public CoWindowDataStream<IN1, IN2> groupBy(int keyPosition1, int keyPosition2) {
		return new CoWindowDataStream<IN1, IN2>(dataStream1.groupBy(keyPosition1),
				dataStream2.groupBy(keyPosition2), batchSize1, batchSize2, slideSize1, slideSize2,
				timeStamp1, timeStamp2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(int[] keyPositions1, int[] keyPositions2) {
		return new CoWindowDataStream<IN1, IN2>(dataStream1.groupBy(keyPositions1),
				dataStream2.groupBy(keyPositions2), batchSize1, batchSize2, slideSize1, slideSize2,
				timeStamp1, timeStamp2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(String field1, String field2) {
		return new CoWindowDataStream<IN1, IN2>(dataStream1.groupBy(field1),
				dataStream2.groupBy(field2), batchSize1, batchSize2, slideSize1, slideSize2,
				timeStamp1, timeStamp2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(String[] fields1, String[] fields2) {
		return new CoWindowDataStream<IN1, IN2>(dataStream1.groupBy(fields1),
				dataStream2.groupBy(fields2), batchSize1, batchSize2, slideSize1, slideSize2,
				timeStamp1, timeStamp2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(KeySelector<IN1, ?> keySelector1,
			KeySelector<IN2, ?> keySelector2) {
		return new CoWindowDataStream<IN1, IN2>(dataStream1.groupBy(keySelector1),
				dataStream2.groupBy(keySelector2), batchSize1, batchSize2, slideSize1, slideSize2,
				timeStamp1, timeStamp2);
	}

	@Override
	protected <OUT> CoInvokable<IN1, IN2, OUT> getReduceInvokable(
			CoReduceFunction<IN1, IN2, OUT> coReducer) {
		CoWindowReduceInvokable<IN1, IN2, OUT> invokable;
		if (isGrouped) {
			invokable = new CoGroupedWindowReduceInvokable<IN1, IN2, OUT>(clean(coReducer),
					batchSize1, batchSize2, slideSize1, slideSize2, keySelector1, keySelector2,
					timeStamp1, timeStamp2);
		} else {
			invokable = new CoWindowReduceInvokable<IN1, IN2, OUT>(clean(coReducer), batchSize1,
					batchSize2, slideSize1, slideSize2, timeStamp1, timeStamp2);
		}
		return invokable;
	}

	protected CoWindowDataStream<IN1, IN2> copy() {
		return new CoWindowDataStream<IN1, IN2>(this);
	}
}
