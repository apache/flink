/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Base class for key aware implementations of {@link TwoInputStreamOperator}.
 *
 * @param <K> The type of the key of this keyed operator
 * @param <IN1> The first input type of the operator
 * @param <IN2> The second input type of the operator
 * @param <OUT> The output type of the operator
 */
@Internal
public abstract class AbstractKeyedTwoInputStreamOperator<K, IN1, IN2, OUT>
		extends AbstractKeyedStreamOperator<K, OUT>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	private final KeySelector<IN1, K> keySelector1;
	private final KeySelector<IN2, K> keySelector2;

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	private long combinedWatermark = Long.MIN_VALUE;
	private long input1Watermark = Long.MIN_VALUE;
	private long input2Watermark = Long.MIN_VALUE;

	public AbstractKeyedTwoInputStreamOperator(
			TypeSerializer<K> keySerializer,
			KeySelector<IN1, K> keySelector1,
			KeySelector<IN2, K> keySelector2) {
		super(keySerializer);
		this.keySelector1 = keySelector1;
		this.keySelector2 = keySelector2;
	}

	public AbstractKeyedTwoInputStreamOperator(
			Function userFunction,
			TypeSerializer<K> keySerializer,
			KeySelector<IN1, K> keySelector1,
			KeySelector<IN2, K> keySelector2) {
		super(userFunction, keySerializer);
		this.keySelector1 = keySelector1;
		this.keySelector2 = keySelector2;
	}

	/**
	 * Processes one element that arrived at the first input of this operator. This method is
	 * guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @param key the key of the element being processed
	 * @param element the element to be processed
	 */
	protected abstract void processKeyedElement1(K key, StreamRecord<IN1> element) throws Exception;

	/**
	 * Processes one element that arrived at the second input this operator. This method is
	 * guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @param key the key of the element being processed
	 * @param element the element to be processed
	 */
	protected abstract void processKeyedElement2(K key, StreamRecord<IN2> element) throws Exception;


	@Override
	public final void processElement1(StreamRecord<IN1> element) throws Exception {
		K key = keySelector1.getKey(element.getValue());

		setCurrentKey(key);

		processKeyedElement1(key, element);

		// unset so that we don't accidentally update state for a key later
		setCurrentKey(null);
	}

	@Override
	public final void processElement2(StreamRecord<IN2> element) throws Exception {
		K key = keySelector2.getKey(element.getValue());

		setCurrentKey(key);

		processKeyedElement2(key, element);

		// unset so that we don't accidentally update state for a key later
		setCurrentKey(null);
	}


	// ------------------------------------------------------------------------
	//  Watermark handling
	// ------------------------------------------------------------------------

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		input1Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			for (HeapInternalTimerService<?, ?> service : timerServices.values()) {
				service.advanceWatermark(mark.getTimestamp());
			}
			output.emitWatermark(mark);
		}
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		input2Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			for (HeapInternalTimerService<?, ?> service : timerServices.values()) {
				service.advanceWatermark(mark.getTimestamp());
			}
			output.emitWatermark(mark);
		}
	}
}
