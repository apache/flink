/**
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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.util.Timestamp;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class WindowReduceInvokable<IN, OUT> extends BatchReduceInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private long startTime;
	private long nextRecordTime;
	private Timestamp<IN> timestamp;

	public WindowReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long windowSize,
			long slideInterval, Timestamp<IN> timestamp) {
		super(reduceFunction, windowSize, slideInterval);
		this.timestamp = timestamp;
	}

	@Override
	protected void immutableInvoke() throws Exception {
		if ((reuse = recordIterator.next(reuse)) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		nextRecordTime = timestamp.getTimestamp(reuse.getObject()); // **
		startTime = nextRecordTime - (nextRecordTime % granularity); // **

		while (reuse != null && !state.isFull()) {
			collectOneUnit();
		}
		reduce();

		while (reuse != null) {
			for (int i = 0; i < slideSize / granularity; i++) {
				if (reuse != null) {
					collectOneUnit();
				}
			}
			reduce();
		}
	}

	@Override
	protected void collectOneUnit() throws IOException {
		ArrayList<StreamRecord<IN>> list;
		if (nextRecordTime > startTime + granularity - 1) {
			list = new ArrayList<StreamRecord<IN>>();
			startTime += granularity;
		} else {
			list = new ArrayList<StreamRecord<IN>>(listSize);

			list.add(reuse);
			resetReuse();

			while ((reuse = recordIterator.next(reuse)) != null && batchNotFull()) {
				list.add(reuse);
				resetReuse();
			}
		}
		state.pushBack(list);
//		System.out.println(list);
//		System.out.println(startTime + " - " + (startTime + granularity - 1) + " ("
//				+ nextRecordTime + ")");
	}

	@Override
	protected boolean batchNotFull() {
		nextRecordTime = timestamp.getTimestamp(reuse.getObject());
		if (nextRecordTime < startTime + granularity) {
			return true;
		} else {
			startTime += granularity;
			return false;
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

	protected void mutableInvoke() throws Exception {
		throw new RuntimeException("Reducing mutable sliding window is not supported.");
	}

}