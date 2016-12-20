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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * A processing time tumbling {@link WindowAssigner window assigner} used to perform windowing using the
 * {@link org.apache.flink.streaming.runtime.operators.windowing.AccumulatingProcessingTimeWindowOperator
 * AccumulatingProcessingTimeWindowOperator} and the
 * {@link org.apache.flink.streaming.runtime.operators.windowing.AggregatingProcessingTimeWindowOperator
 * AggregatingProcessingTimeWindowOperator}.
 *
 * <p>
 * With this assigner, the {@code trigger} used is a
 * {@link org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger
 * ProcessingTimeTrigger} and no {@code evictor} can be specified.
 *
 * <p>
 * <b>WARNING:</b> Bear in mind that no rescaling and no backwards compatibility is supported.
 * */
public class TumblingAlignedProcessingTimeWindows extends BaseAlignedWindowAssigner {

	private static final long serialVersionUID = -6217477609512299842L;

	protected TumblingAlignedProcessingTimeWindows(long size) {
		super(size);
	}

	/**
	 * Creates a new {@code TumblingAlignedProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp.
	 *
	 * @param size The size of the generated windows.
	 */
	public static TumblingAlignedProcessingTimeWindows of(Time size) {
		return new TumblingAlignedProcessingTimeWindows(size.toMilliseconds());
	}
}
