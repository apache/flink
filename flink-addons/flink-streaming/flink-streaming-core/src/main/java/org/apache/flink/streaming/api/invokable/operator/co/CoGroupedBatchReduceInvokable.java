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

package org.apache.flink.streaming.api.invokable.operator.co;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class CoGroupedBatchReduceInvokable<IN1, IN2, OUT> extends
		CoBatchReduceInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;
	int keyPosition1;
	int keyPosition2;
	Map<Object, StreamBatch<IN1>> streamBatches1;
	Map<Object, StreamBatch<IN2>> streamBatches2;

	public CoGroupedBatchReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer,
			long batchSize1, long batchSize2, long slideSize1, long slideSize2, int keyPosition1,
			int keyPosition2) {
		super(coReducer, batchSize1, batchSize2, slideSize1, slideSize2);
		this.keyPosition1 = keyPosition1;
		this.keyPosition2 = keyPosition2;
		this.streamBatches1 = new HashMap<Object, StreamBatch<IN1>>();
		this.streamBatches2 = new HashMap<Object, StreamBatch<IN2>>();
	}

	protected void reduceLastBatch1() throws Exception {
		for (StreamBatch<IN1> batch : streamBatches1.values()) {
			reduceLastBatch1(batch);
		}
	}

	protected void reduceLastBatch2() throws Exception {
		for (StreamBatch<IN2> batch : streamBatches2.values()) {
			reduceLastBatch2(batch);
		}
	}

	@Override
	protected StreamBatch<IN1> getBatch1(StreamRecord<IN1> next) {
		Object key = next.getField(keyPosition1);
		StreamBatch<IN1> batch = streamBatches1.get(key);
		if (batch == null) {
			batch = new StreamBatch<IN1>(batchSize1, slideSize1);
			streamBatches1.put(key, batch);
		}
		return batch;
	}

	@Override
	protected StreamBatch<IN2> getBatch2(StreamRecord<IN2> next) {
		Object key = next.getField(keyPosition2);
		StreamBatch<IN2> batch = streamBatches2.get(key);
		if (batch == null) {
			batch = new StreamBatch<IN2>(batchSize2, slideSize2);
			streamBatches2.put(key, batch);
		}
		return batch;
	}

}
