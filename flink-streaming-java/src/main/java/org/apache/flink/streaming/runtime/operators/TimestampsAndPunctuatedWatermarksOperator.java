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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates watermarks based on punctuation elements.
 *
 * @param <T> The type of the input elements
 */
public class TimestampsAndPunctuatedWatermarksOperator<T>
		extends AbstractUdfStreamOperator<T, AssignerWithPunctuatedWatermarks<T>>
		implements OneInputStreamOperator<T, T> {

	private static final long serialVersionUID = 1L;

	private transient long currentWatermark;

	
	public TimestampsAndPunctuatedWatermarksOperator(AssignerWithPunctuatedWatermarks<T> assigner) {
		super(assigner);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		currentWatermark = -1L;
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		final T value = element.getValue();
		final long newTimestamp = userFunction.extractTimestamp(value, element.getTimestamp());
		output.collect(element.replace(element.getValue(), newTimestamp));
		
		final long nextWatermark = userFunction.checkAndGetNextWatermark(value, newTimestamp);
		if (nextWatermark >= 0 && nextWatermark > currentWatermark) {
			currentWatermark = nextWatermark;
			output.emitWatermark(new Watermark(nextWatermark));
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}
}
