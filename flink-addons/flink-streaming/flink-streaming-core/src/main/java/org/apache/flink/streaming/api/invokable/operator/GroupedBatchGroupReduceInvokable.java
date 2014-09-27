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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.MutableTableState;

public class GroupedBatchGroupReduceInvokable<IN, OUT> extends BatchGroupReduceInvokable<IN, OUT> {

	private static final long serialVersionUID = 1L;

	int keyPosition;
	private Iterator<StreamRecord<IN>> iterator;
	private MutableTableState<Object, List<IN>> values;

	public GroupedBatchGroupReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long batchSize,
			long slideSize, int keyPosition) {
		super(reduceFunction, batchSize, slideSize);
		this.keyPosition = keyPosition;
		this.reducer = reduceFunction;
		values = new MutableTableState<Object, List<IN>>();
	}

	private IN nextValue;

	@Override
	protected void reduce() {
		iterator = state.getStreamRecordIterator();
		while (iterator.hasNext()) {
			StreamRecord<IN> nextRecord = iterator.next();
			Object key = nextRecord.getField(keyPosition);
			nextValue = nextRecord.getObject();

			List<IN> group = values.get(key);
			if (group != null) {
				group.add(nextValue);
			} else {
				group = new ArrayList<IN>();
				group.add(nextValue);
				values.put(key, group);
			}
		}
		for (List<IN> group : values.values()) {
			userIterable = group;
			callUserFunctionAndLogException();
		}
		values.clear();
	}

}
