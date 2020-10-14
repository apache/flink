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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The implementation of {@link InternalWindowProcessFunction} for {@link MergingWindowAssigner}.
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public abstract class MergingWindowProcessFunction<K, W extends Window>
		extends AbstractWindowProcessFunction<K, W> {

	private static final long serialVersionUID = -2866771637946397223L;

	private final String mergingWindowStateName;

	protected final MergingWindowAssigner<W> windowAssigner;
	private final TypeSerializer<W> windowSerializer;
	protected transient MergingWindowSet<W> mergingWindows;
	private transient MergingFunctionImpl mergingFunction;

	private List<W> reuseActualWindows;

	public MergingWindowProcessFunction(
			MergingWindowAssigner<W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			long allowedLateness) {
		this("session-window-mapping", windowAssigner,
				windowSerializer, allowedLateness);
	}

	public MergingWindowProcessFunction(
			String mergingWindowStateName,
			MergingWindowAssigner<W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			long allowedLateness) {
		super(windowAssigner, allowedLateness);
		this.mergingWindowStateName = mergingWindowStateName;
		this.windowAssigner = windowAssigner;
		this.windowSerializer = windowSerializer;
	}

	@Override
	public void open(Context<K, W> ctx) throws Exception {
		super.open(ctx);
		MapStateDescriptor<W, W> mappingStateDescriptor = new MapStateDescriptor<>(
			mergingWindowStateName,
			windowSerializer,
			windowSerializer);
		MapState<W, W> windowMapping = ctx.getPartitionedState(mappingStateDescriptor);
		this.mergingWindows = new MergingWindowSet<>(windowAssigner, windowMapping);
		this.mergingFunction = new MergingFunctionImpl();
	}

	@Override
	public Collection<W> assignStateNamespace(RowData inputRow, long timestamp) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(inputRow, timestamp);
		mergingWindows.initializeCache(ctx.currentKey());
		reuseActualWindows = new ArrayList<>(1);
		for (W window : elementWindows) {
			// adding the new window might result in a merge, in that case the actualWindow
			// is the merged window and we work with that. If we don't merge then
			// actualWindow == window
			W actualWindow = mergingWindows.addWindow(window, mergingFunction);

			// drop if the window is already late
			if (isWindowLate(actualWindow)) {
				mergingWindows.retireWindow(actualWindow);
			} else {
				reuseActualWindows.add(actualWindow);
			}
		}
		List<W> affectedWindows = new ArrayList<>(reuseActualWindows.size());
		for (W actual : reuseActualWindows) {
			affectedWindows.add(mergingWindows.getStateWindow(actual));
		}
		return affectedWindows;
	}

	@Override
	public Collection<W> assignActualWindows(RowData inputRow, long timestamp) throws Exception {
		// the actual windows is calculated in assignStateNamespace
		return reuseActualWindows;
	}

	@Override
	public void cleanWindowIfNeeded(W window, long currentTime) throws Exception {
		if (isCleanupTime(window, currentTime)) {
			ctx.clearTrigger(window);
			W stateWindow = mergingWindows.getStateWindow(window);
			ctx.clearWindowState(stateWindow);
			// retire expired window
			mergingWindows.initializeCache(ctx.currentKey());
			mergingWindows.retireWindow(window);
			// do not need to clear previous state, previous state is disabled in session window
		}
	}

	/**
	 * Callback for {@code MergingWindowSet.MergeFunction} to merge the
	 * namespaces of state windows.
	 *
	 * @param stateWindowResult      Result state window
	 * @param stateWindowsToBeMerged State window to be merged
	 */
	protected abstract void mergeNamespaces(
			W stateWindowResult,
			Collection<W> stateWindowsToBeMerged) throws Exception;

	private class MergingFunctionImpl implements MergingWindowSet.MergeFunction<W> {

		@Override
		public void merge(
			W mergeResult,
			Collection<W> mergedWindows,
			W stateWindowResult,
			Collection<W> stateWindowsToBeMerged) throws Exception {

			if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= ctx.currentWatermark())) {
				throw new UnsupportedOperationException("The end timestamp of an " +
					"event-time window cannot become earlier than the current watermark " +
					"by merging. Current watermark: " + ctx.currentWatermark() +
					" window: " + mergeResult);
			} else if (!windowAssigner.isEventTime() && mergeResult.maxTimestamp() <= ctx.currentProcessingTime()) {
				throw new UnsupportedOperationException("The end timestamp of a " +
					"processing-time window cannot become earlier than the current processing time " +
					"by merging. Current processing time: " + ctx.currentProcessingTime() +
					" window: " + mergeResult);
			}

			ctx.onMerge(mergeResult, stateWindowsToBeMerged);

			// clear registered timers
			for (W m : mergedWindows) {
				ctx.clearTrigger(m);
				ctx.deleteCleanupTimer(m);
			}

			// merge the merged state windows into the newly resulting state window
			mergeNamespaces(stateWindowResult, stateWindowsToBeMerged);
		}
	}
}
