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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.MutableTableState;

public class CoGroupedWindowGroupReduceInvokable<IN1, IN2, OUT> extends
		CoWindowGroupReduceInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private int keyPosition1;
	private int keyPosition2;
	private Iterator<StreamRecord<IN1>> iterator1;
	private Iterator<StreamRecord<IN2>> iterator2;
	private MutableTableState<Object, List<IN1>> values1;
	private MutableTableState<Object, List<IN2>> values2;
	private IN1 nextValue1;
	private IN2 nextValue2;

	public CoGroupedWindowGroupReduceInvokable(
			CoGroupReduceFunction<IN1, IN2, OUT> coReduceFunction, long windowSize1,
			long windowSize2, long slideInterval1, long slideInterval2, int keyPosition1,
			int keyPosition2, TimeStamp<IN1> timestamp1, TimeStamp<IN2> timestamp2) {
		super(coReduceFunction, windowSize1, windowSize2, slideInterval1, slideInterval2,
				timestamp1, timestamp2);
		this.keyPosition1 = keyPosition1;
		this.keyPosition2 = keyPosition2;
		values1 = new MutableTableState<Object, List<IN1>>();
		values2 = new MutableTableState<Object, List<IN2>>();
	}

	@Override
	protected void reduce1() {
		iterator1 = circularList1.getIterator();
		while (iterator1.hasNext()) {
			StreamRecord<IN1> nextRecord = iterator1.next();
			Object key = nextRecord.getField(keyPosition1);
			nextValue1 = nextRecord.getObject();

			List<IN1> group = values1.get(key);
			if (group != null) {
				group.add(nextValue1);
			} else {
				group = new ArrayList<IN1>();
				group.add(nextValue1);
				values1.put(key, group);
			}
		}
		for (List<IN1> group : values1.values()) {
			userIterable1 = group;
			callUserFunctionAndLogException1();
		}
		values1.clear();
	}

	@Override
	protected void reduce2() {
		iterator2 = circularList2.getIterator();
		while (iterator2.hasNext()) {
			StreamRecord<IN2> nextRecord = iterator2.next();
			Object key = nextRecord.getField(keyPosition2);
			nextValue2 = nextRecord.getObject();

			List<IN2> group = values2.get(key);
			if (group != null) {
				group.add(nextValue2);
			} else {
				group = new ArrayList<IN2>();
				group.add(nextValue2);
				values2.put(key, group);
			}
		}
		for (List<IN2> group : values2.values()) {
			userIterable2 = group;
			callUserFunctionAndLogException2();
		}
		values2.clear();
	}

}
