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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;

/**
 * A base {@link WindowAssigner} used to instantiate one of the deprecated
 * {@link org.apache.flink.streaming.runtime.operators.windowing.AccumulatingProcessingTimeWindowOperator
 * AccumulatingProcessingTimeWindowOperator} and
 * {@link org.apache.flink.streaming.runtime.operators.windowing.AggregatingProcessingTimeWindowOperator
 * AggregatingProcessingTimeWindowOperator}.
 *
 * <p>For assigner that extend this one, the user can check the
 * {@link TumblingAlignedProcessingTimeWindows} and the {@link SlidingAlignedProcessingTimeWindows}.
 * */
public class BaseAlignedWindowAssigner extends WindowAssigner<Object, TimeWindow> {

	private static final long serialVersionUID = -6214980179706960234L;

	private final long size;

	protected BaseAlignedWindowAssigner(long size) {
		this.size = size;
	}

	public long getSize() {
		return size;
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		throw new UnsupportedOperationException("This assigner should not be used with the WindowOperator.");
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return null;
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		throw new UnsupportedOperationException("This assigner should not be used with the WindowOperator.");
	}

	@Override
	public boolean isEventTime() {
		throw new UnsupportedOperationException("This assigner should not be used with the WindowOperator.");
	}
}
