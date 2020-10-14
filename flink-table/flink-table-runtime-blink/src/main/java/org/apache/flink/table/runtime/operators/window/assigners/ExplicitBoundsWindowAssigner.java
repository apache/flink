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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into the given window.
 */
public class ExplicitBoundsWindowAssigner
		extends WindowAssigner<TimeWindow>
		implements InternalTimeWindowAssigner {

	private static final long serialVersionUID = 1L;

	/**
	 * Attribute window-start index.
	 */
	private final int windowStartRef;

	/**
	 * Attribute window-end index.
	 */
	private final int windowEndRef;

	/**
	 * Whether the time attribute is event-time.
	 */
	private final boolean isEventTime;

	protected ExplicitBoundsWindowAssigner(
			int windowStartRef,
			int windowEndRef,
			boolean isEventTime) {
		this.windowStartRef = windowStartRef;
		this.windowEndRef = windowEndRef;
		this.isEventTime = isEventTime;
	}

	@Override
	public Collection<TimeWindow> assignWindows(RowData element, long timestamp) {
		TimeWindow window = new TimeWindow(element.getLong(windowStartRef),
				element.getLong(windowEndRef));
		return Collections.singletonList(window);
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return isEventTime;
	}

	@Override
	public String toString() {
		return String.format("ExplicitBoundsWindow(%d, %d)", windowStartRef, windowEndRef);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code ExplicitBoundsWindowAssigner} that assigns
	 * elements to the given time window.
	 *
	 * @return The time policy.
	 */
	public static ExplicitBoundsWindowAssigner of(int windowStartRef, int windowEndRef) {
		return new ExplicitBoundsWindowAssigner(windowStartRef, windowEndRef, true);
	}

	public ExplicitBoundsWindowAssigner withEventTime() {
		return new ExplicitBoundsWindowAssigner(windowStartRef, windowEndRef, true);
	}

	public ExplicitBoundsWindowAssigner withProcessingTime() {
		return new ExplicitBoundsWindowAssigner(windowStartRef, windowEndRef, false);
	}
}
