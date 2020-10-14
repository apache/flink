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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * The implementation of {@link InternalWindowProcessFunction} for
 * window aggregate {@link MergingWindowAssigner}.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class AggregateMergingWindowProcessFunction<K, W extends Window>
		extends MergingWindowProcessFunction<K, W>
		implements WindowAggregateProcessFunction<K, W> {

	private static final long serialVersionUID = -2866771637946397223L;

	private final NamespaceAggsHandleFunctionBase<W> windowAggregator;
	// A typed copy to have aggregate specific operations.
	protected WindowAggregateProcessFunction.Context<K, W> ctx;

	public AggregateMergingWindowProcessFunction(
			MergingWindowAssigner<W> windowAssigner,
			NamespaceAggsHandleFunctionBase<W> windowAggregator,
			TypeSerializer<W> windowSerializer,
			long allowedLateness) {
		super(windowAssigner, windowSerializer, allowedLateness);
		this.windowAggregator = windowAggregator;
	}

	@Override
	public void open(InternalWindowProcessFunction.Context<K, W> ctx) throws Exception {
		Preconditions.checkArgument(ctx instanceof WindowAggregateProcessFunction.Context,
				"The context must implement WindowAggregateProcessFunction.Context");
		super.open(ctx);
		this.ctx = (WindowAggregateProcessFunction.Context<K, W>) ctx;
	}

	public void prepareAggregateAccumulatorForEmit(W window) throws Exception {
		W stateWindow = mergingWindows.getStateWindow(window);
		RowData acc = ctx.getWindowAccumulators(stateWindow);
		if (acc == null) {
			acc = windowAggregator.createAccumulators();
		}
		windowAggregator.setAccumulators(stateWindow, acc);
	}

	@Override
	protected void mergeNamespaces(W stateWindowResult, Collection<W> stateWindowsToBeMerged)
			throws Exception {
		if (!stateWindowsToBeMerged.isEmpty()) {
			RowData targetAcc = ctx.getWindowAccumulators(stateWindowResult);
			if (targetAcc == null) {
				targetAcc = windowAggregator.createAccumulators();
			}
			windowAggregator.setAccumulators(stateWindowResult, targetAcc);
			for (W w : stateWindowsToBeMerged) {
				RowData acc = ctx.getWindowAccumulators(w);
				if (acc != null) {
					windowAggregator.merge(w, acc);
				}
				// clear merged window
				ctx.clearWindowState(w);
				ctx.clearPreviousState(w);
			}
			targetAcc = windowAggregator.getAccumulators();
			ctx.setWindowAccumulators(stateWindowResult, targetAcc);
		}
	}
}
