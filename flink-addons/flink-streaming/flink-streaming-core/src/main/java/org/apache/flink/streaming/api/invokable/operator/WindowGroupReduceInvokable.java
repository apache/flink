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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.Iterator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.MutableTableState;

public class WindowGroupReduceInvokable<IN> extends WindowReduceInvokable<IN, IN> {

	int keyPosition;
	protected ReduceFunction<IN> reducer;
	private Iterator<StreamRecord<IN>> iterator;
	private MutableTableState<Object, IN> values;

	public WindowGroupReduceInvokable(ReduceFunction<IN> reduceFunction, long windowSize,
			long slideInterval, int keyPosition) {
		super(reduceFunction, windowSize, slideInterval);
		this.keyPosition = keyPosition;
		this.reducer = reduceFunction;
		values = new MutableTableState<Object, IN>();
	}

	@Override
	protected void reduce() throws Exception {
		iterator = state.getStreamRecordIterator();
		while (iterator.hasNext()) {
			StreamRecord<IN> nextRecord = iterator.next();

			IN nextValue = nextRecord.getObject();
			Object key = nextRecord.getField(keyPosition);

			IN currentValue = values.get(key);
			if (currentValue != null) {
				IN reduced = reducer.reduce(currentValue, nextValue);
				values.put(key, reduced);
				collector.collect(reduced);
			} else {
				values.put(key, nextValue);
				collector.collect(nextValue);
			}
		}
		values.clear();
	}

	private static final long serialVersionUID = 1L;

}