/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * This mapper validates exactly-once and at-least-once semantics in connection with {@link SequenceGeneratorSource}.
 */
public class SemanticsCheckMapper extends RichFlatMapFunction<Event, String> {

	private static final long serialVersionUID = -744070793650644485L;

	/** This value state tracks the current sequence number per key. */
	private volatile ValueState<Long> sequenceValue;

	/** This defines how semantics are checked for each update. */
	private final ValidatorFunction validator;

	SemanticsCheckMapper(ValidatorFunction validator) {
		this.validator = validator;
	}

	@Override
	public void flatMap(Event event, Collector<String> out) throws Exception {

		Long currentValue = sequenceValue.value();
		if (currentValue == null) {
			currentValue = 0L;
		}

		long nextValue = event.getSequenceNumber();

		sequenceValue.update(nextValue);
		if (!validator.check(currentValue, nextValue)) {
			out.collect("Alert: " + currentValue + " -> " + nextValue + " (" + event.getKey() + ")");
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Long> sequenceStateDescriptor =
			new ValueStateDescriptor<>("sequenceState", Long.class);

		sequenceValue = getRuntimeContext().getState(sequenceStateDescriptor);
	}

	interface ValidatorFunction extends Serializable {
		boolean check(long current, long update);

		static ValidatorFunction exactlyOnce() {
			return (current, update) -> (update - current) == 1;
		}

		static ValidatorFunction atLeastOnce() {
			return (current, update) -> (update - current) <= 1;
		}
	}
}
