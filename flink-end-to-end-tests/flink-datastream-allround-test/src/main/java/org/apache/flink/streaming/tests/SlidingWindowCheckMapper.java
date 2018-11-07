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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 * This mapper validates sliding event time window. It checks each event belongs to appropriate number of consecutive windows.
 */
public class SlidingWindowCheckMapper extends RichFlatMapFunction<Tuple2<Integer, List<Event>>, String> {

	private static final long serialVersionUID = -744070793650644485L;

	/** This value state tracks previously seen events with the number of windows they appeared in. */
	private transient ValueState<List<Tuple2<Event, Integer>>> previousWindow;

	private final int slideFactor;

	SlidingWindowCheckMapper(int slideFactor) {
		this.slideFactor = slideFactor;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<List<Tuple2<Event, Integer>>> previousWindowDescriptor =
			new ValueStateDescriptor<>("previousWindow",
				new ListTypeInfo<>(new TupleTypeInfo<>(TypeInformation.of(Event.class), BasicTypeInfo.INT_TYPE_INFO)));

		previousWindow = getRuntimeContext().getState(previousWindowDescriptor);
	}

	@Override
	public void flatMap(Tuple2<Integer, List<Event>> value, Collector<String> out) throws Exception {
		List<Tuple2<Event, Integer>> previousWindowValues = Optional.ofNullable(previousWindow.value()).orElseGet(
			Collections::emptyList);

		List<Event> newValues = value.f1;
		newValues.stream().reduce(new BinaryOperator<Event>() {
			@Override
			public Event apply(Event event, Event event2) {
				if (event2.getSequenceNumber() - 1 != event.getSequenceNumber()) {
					out.collect("Alert: events in window out ouf order!");
				}

				return event2;
			}
		});

		List<Tuple2<Event, Integer>> newWindow = new ArrayList<>();
		for (Tuple2<Event, Integer> windowValue : previousWindowValues) {
			if (!newValues.contains(windowValue.f0)) {
				out.collect(String.format("Alert: event %s did not belong to %d consecutive windows. Event seen so far %d times.Current window: %s",
					windowValue.f0,
					slideFactor,
					windowValue.f1,
					value.f1));
			} else {
				newValues.remove(windowValue.f0);
				if (windowValue.f1 + 1 != slideFactor) {
					newWindow.add(Tuple2.of(windowValue.f0, windowValue.f1 + 1));
				}
			}
		}

		newValues.forEach(e -> newWindow.add(Tuple2.of(e, 1)));

		previousWindow.update(newWindow);
	}
}
