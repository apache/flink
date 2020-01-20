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

package org.apache.flink.basic.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Function that concatenates elements by the concatenation of all preceding elements.
 */
public class ValueStateFlatMap extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {
	private transient ValueState<String> contactValues;

	@Override
	public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> collector) throws Exception {
		String contactValue = contactValues.value() + value.f1;
		contactValues.update(contactValue);
		collector.collect(new Tuple2<>(value.f0, contactValue));
	}

	@Override
	public void open(Configuration config) {
		final ValueStateDescriptor<String> descriptor =
			new ValueStateDescriptor<>("sumValues", TypeInformation.of(new TypeHint<String>() {}), "");
		contactValues = getRuntimeContext().getState(descriptor);
	}
}
