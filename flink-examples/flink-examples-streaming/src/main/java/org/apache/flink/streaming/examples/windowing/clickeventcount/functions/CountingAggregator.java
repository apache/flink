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

package org.apache.flink.streaming.examples.windowing.clickeventcount.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.examples.windowing.clickeventcount.records.ClickEvent;

/**
 * An {@link AggregateFunction} which simply counts {@link ClickEvent}s.
 *
 */
public class CountingAggregator implements AggregateFunction<ClickEvent, Long, Long> {
	@Override
	public Long createAccumulator() {
		return 0L;
	}

	@Override
	public Long add(final ClickEvent value, final Long accumulator) {
		return accumulator + 1;
	}

	@Override
	public Long getResult(final Long accumulator) {
		return accumulator;
	}

	@Override
	public Long merge(final Long a, final Long b) {
		return a + b;
	}
}
