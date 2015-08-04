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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class JumpingCountGroupedPreReducer<T> extends TumblingGroupedPreReducer<T> {

	private static final long serialVersionUID = 1L;

	private final long countToSkip; // How many elements should be jumped over
	private long skipped = 0; // How many elements have we skipped since the last emitWindow

	public JumpingCountGroupedPreReducer(ReduceFunction<T> reducer, KeySelector<T, ?> keySelector,
										TypeSerializer<T> serializer, long countToSkip) {
		super(reducer, keySelector, serializer);
		this.countToSkip = countToSkip;
	}

	@Override
	public void emitWindow(Collector<StreamRecord<StreamWindow<T>>> collector) {
		super.emitWindow(collector);
		skipped = 0;
	}

	@Override
	public void store(T element) throws Exception {
		if(skipped == countToSkip){
			super.store(element);
		} else {
			skipped++;
		}
	}
}
