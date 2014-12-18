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
import org.apache.flink.streaming.api.invokable.operator.co.CoBatchReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedBatchReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;

/**
 * A {@link CoBatchedDataStream} represents a two data stream whose elements are
 * batched together in sliding batches. Operation
 * {@link #reduce(ReduceFunction)} can be applied for each batch and the batch
 * is slid afterwards.
 *
 * @param <IN1>
 *            The type of the first input data stream
 * @param <IN2>
 *            The type of the second input data stream
 */
public class CoBatchedDataStream<IN1, IN2> extends ConnectedDataStream<IN1, IN2> {

	protected long batchSize1;
	protected long batchSize2;
	protected long slideSize1;
	protected long slideSize2;

	protected CoBatchedDataStream(DataStream<IN1> dataStream1, DataStream<IN2> dataStream2,
			long batchSize1, long batchSize2, long slideSize1, long slideSize2) {
		super(dataStream1, dataStream2);
		this.batchSize1 = batchSize1;
		this.batchSize2 = batchSize2;
		this.slideSize1 = slideSize1;
		this.slideSize2 = slideSize2;
	}

	protected CoBatchedDataStream(ConnectedDataStream<IN1, IN2> coDataStream, long batchSize1,
			long batchSize2, long slideSize1, long slideSize2) {
		super(coDataStream);
		this.batchSize1 = batchSize1;
		this.batchSize2 = batchSize2;
		this.slideSize1 = slideSize1;
		this.slideSize2 = slideSize2;
	}

	protected CoBatchedDataStream(CoBatchedDataStream<IN1, IN2> coBatchedDataStream) {
		super(coBatchedDataStream);
		this.batchSize1 = coBatchedDataStream.batchSize1;
		this.batchSize2 = coBatchedDataStream.batchSize2;
		this.slideSize1 = coBatchedDataStream.slideSize1;
		this.slideSize2 = coBatchedDataStream.slideSize2;
	}

	/**
	 * Groups the elements of the {@link CoBatchedDataStream} by the given key
	 * positions to be used with grouped operators.
	 * 
	 * @param keyPosition1
	 *            The position of the field on which the first input data stream
	 *            will be grouped.
	 * @param keyPosition2
	 *            The position of the field on which the second input data
	 *            stream will be grouped.
	 * @return The transformed {@link CoBatchedDataStream}
	 */
	public CoBatchedDataStream<IN1, IN2> groupBy(int keyPosition1, int keyPosition2) {
		return new CoBatchedDataStream<IN1, IN2>(dataStream1.groupBy(keyPosition1),
				dataStream2.groupBy(keyPosition2), batchSize1, batchSize2, slideSize1, slideSize2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(int[] keyPositions1, int[] keyPositions2) {
		return new CoBatchedDataStream<IN1, IN2>(dataStream1.groupBy(keyPositions1),
				dataStream2.groupBy(keyPositions2), batchSize1, batchSize2, slideSize1, slideSize2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(String field1, String field2) {
		return new CoBatchedDataStream<IN1, IN2>(dataStream1.groupBy(field1),
				dataStream2.groupBy(field2), batchSize1, batchSize2, slideSize1, slideSize2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(String[] fields1, String[] fields2) {
		return new CoBatchedDataStream<IN1, IN2>(dataStream1.groupBy(fields1),
				dataStream2.groupBy(fields2), batchSize1, batchSize2, slideSize1, slideSize2);
	}

	public ConnectedDataStream<IN1, IN2> groupBy(KeySelector<IN1, ?> keySelector1,
			KeySelector<IN2, ?> keySelector2) {
		return new CoBatchedDataStream<IN1, IN2>(dataStream1.groupBy(keySelector1),
				dataStream2.groupBy(keySelector2), batchSize1, batchSize2, slideSize1, slideSize2);
	}

	@Override
	protected <OUT> CoInvokable<IN1, IN2, OUT> getReduceInvokable(
			CoReduceFunction<IN1, IN2, OUT> coReducer) {
		CoBatchReduceInvokable<IN1, IN2, OUT> invokable;
		if (isGrouped) {
			invokable = new CoGroupedBatchReduceInvokable<IN1, IN2, OUT>(clean(coReducer), batchSize1,
					batchSize2, slideSize1, slideSize2, keySelector1, keySelector2);
		} else {
			invokable = new CoBatchReduceInvokable<IN1, IN2, OUT>(clean(coReducer), batchSize1,
					batchSize2, slideSize1, slideSize2);
		}
		return invokable;
	}

	protected CoBatchedDataStream<IN1, IN2> copy() {
		return new CoBatchedDataStream<IN1, IN2>(this);
	}

}
