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

package org.apache.flink.table.runtime.operators.window.internal;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;

import java.io.Serializable;
import java.util.Collection;

/**
 * The internal interface for functions that process over grouped windows.
 *
 * @param <K> type of the context key
 * @param <W> type of window
 */
public abstract class AbstractWindowProcessFunction<K, W extends Window>
		implements InternalWindowProcessFunction<K, W>, Serializable {

	private static final long serialVersionUID = 5191040787066951059L;

	protected final WindowAssigner<W> windowAssigner;
	protected final long allowedLateness;
	protected Context<K, W> ctx;

	protected AbstractWindowProcessFunction(
			WindowAssigner<W> windowAssigner,
			long allowedLateness) {
		this.windowAssigner = windowAssigner;
		this.allowedLateness = allowedLateness;
	}

	/**
	 * Initialization method for the function. It is called before the actual working methods.
	 */
	public void open(Context<K, W> ctx) throws Exception {
		this.ctx = ctx;
		this.windowAssigner.open(ctx);
	}

	/**
	 * Assigns the input element into the state namespace which the input element should be
	 * accumulated/retracted into.
	 *
	 * @param inputRow  the input element
	 * @param timestamp the timestamp of the element or the processing time (depends on the type of
	 *                  assigner)
	 * @return the state namespace
	 */
	public abstract Collection<W> assignStateNamespace(RowData inputRow,
			long timestamp) throws Exception;

	/**
	 * Assigns the input element into the actual windows which the {@link Trigger} should trigger
	 * on.
	 *
	 * @param inputRow  the input element
	 * @param timestamp the timestamp of the element or the processing time (depends on the type of
	 *                  assigner)
	 * @return the actual windows
	 */
	public abstract Collection<W> assignActualWindows(RowData inputRow,
			long timestamp) throws Exception;

	/**
	 * Cleans the given window if needed.
	 *
	 * @param window      the window to cleanup
	 * @param currentTime the current timestamp
	 */
	public abstract void cleanWindowIfNeeded(W window, long currentTime) throws Exception;

	/**
	 * The tear-down method of the function. It is called after the last call to the main working
	 * methods.
	 */
	public void close() throws Exception {
		// default do nothing.
	}

	/**
	 * Returns {@code true} if the given time is the cleanup time for the given window.
	 */
	protected final boolean isCleanupTime(W window, long time) {
		return time == cleanupTime(window);
	}

	/**
	 * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness
	 * of the given window.
	 */
	protected boolean isWindowLate(W window) {
		return (windowAssigner.isEventTime() && (cleanupTime(window) <= ctx.currentWatermark()));
	}

	/**
	 * Returns the cleanup time for a window, which is
	 * {@code window.maxTimestamp + allowedLateness}. In
	 * case this leads to a value greated than {@link Long#MAX_VALUE}
	 * then a cleanup time of {@link Long#MAX_VALUE} is
	 * returned.
	 *
	 * @param window the window whose cleanup time we are computing.
	 */
	private long cleanupTime(W window) {
		if (windowAssigner.isEventTime()) {
			long cleanupTime = window.maxTimestamp() + allowedLateness;
			return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
		} else {
			return window.maxTimestamp();
		}
	}
}
