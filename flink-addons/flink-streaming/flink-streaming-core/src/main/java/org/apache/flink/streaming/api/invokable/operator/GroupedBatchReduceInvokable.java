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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class GroupedBatchReduceInvokable<OUT> extends BatchReduceInvokable<OUT> {

	private static final long serialVersionUID = 1L;
	int keyPosition;
	Map<Object, StreamBatch> streamBatches;

	public GroupedBatchReduceInvokable(ReduceFunction<OUT> reduceFunction, long batchSize,
			long slideSize, int keyPosition) {
		super(reduceFunction, batchSize, slideSize);
		this.keyPosition = keyPosition;
		this.streamBatches = new HashMap<Object, StreamBatch>();
	}

	@Override
	protected StreamBatch getBatch(StreamRecord<OUT> next) {
		Object key = next.getField(keyPosition);
		StreamBatch batch = streamBatches.get(key);
		if (batch == null) {
			batch = new StreamBatch();
			streamBatches.put(key, batch);
		}
		return batch;
	}

	@Override
	protected void reduceLastBatch() throws Exception {
		for (StreamBatch batch : streamBatches.values()) {
			batch.reduceLastBatch();
		}
	}

}
