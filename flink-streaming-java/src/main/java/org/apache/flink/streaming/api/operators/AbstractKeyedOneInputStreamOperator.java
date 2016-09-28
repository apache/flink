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
 * Base class for key aware implementations of {@link OneInputStreamOperator}.
 *
 * @param <K> The type of the key of this keyed operator
 * @param <IN> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@Internal
public abstract class AbstractKeyedOneInputStreamOperator<K, IN, OUT>
		extends AbstractKeyedStreamOperator<K, OUT>
		implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private final KeySelector<IN, K> keySelector;

	public AbstractKeyedOneInputStreamOperator(
			TypeSerializer<K> keySerializer,
			KeySelector<IN, K> keySelector) {
		super(keySerializer);
		this.keySelector = keySelector;
	}

	public AbstractKeyedOneInputStreamOperator(
			Function userFunction,
			TypeSerializer<K> keySerializer,
			KeySelector<IN, K> keySelector) {
		super(userFunction, keySerializer);
		this.keySelector = keySelector;
	}

	/**
	 * Processes one element that arrived at this operator. This method is guaranteed to not
	 * be called concurrently with other methods of the operator.
	 *
	 * @param key the key of the element being processed
	 * @param element the element to be processed
	 */
	protected abstract void processKeyedElement(K key, StreamRecord<IN> element) throws Exception;

	@Override
	public final void processElement(StreamRecord<IN> element) throws Exception {
		K key = keySelector.getKey(element.getValue());

		setCurrentKey(key);

		processKeyedElement(key, element);

		// unset so that we don't accidentally update state for a key later
		setCurrentKey(null);
	}

	public void processWatermark(Watermark mark) throws Exception {
		for (HeapInternalTimerService<?, ?> service : timerServices.values()) {
			service.advanceWatermark(mark.getTimestamp());
		}
		output.emitWatermark(mark);
	}
}
