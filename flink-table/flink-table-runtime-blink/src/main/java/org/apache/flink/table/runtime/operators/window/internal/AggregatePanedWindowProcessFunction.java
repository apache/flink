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
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.PanedWindowAssigner;
import org.apache.flink.util.Preconditions;

/**
 * The implementation of {@link InternalWindowProcessFunction} for
 * window aggregate {@link PanedWindowAssigner}.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class AggregatePanedWindowProcessFunction<K, W extends Window>
		extends PanedWindowProcessFunction<K, W>
		implements WindowAggregateProcessFunction<K, W> {

	private static final long serialVersionUID = 4259335376102569987L;

	private final NamespaceAggsHandleFunctionBase<W> windowAggregator;
	private final PanedWindowAssigner<W> windowAssigner;
	// A typed copy to have aggregate specific operations.
	private WindowAggregateProcessFunction.Context<K, W> ctx;

	public AggregatePanedWindowProcessFunction(
			PanedWindowAssigner<W> windowAssigner,
			NamespaceAggsHandleFunctionBase<W> windowAggregator,
			long allowedLateness) {
		super(windowAssigner, allowedLateness);
		this.windowAssigner = windowAssigner;
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
		Iterable<W> panes = windowAssigner.splitIntoPanes(window);
		RowData acc = windowAggregator.createAccumulators();
		// null namespace means use heap data views
		windowAggregator.setAccumulators(null, acc);
		for (W pane : panes) {
			RowData paneAcc = ctx.getWindowAccumulators(pane);
			if (paneAcc != null) {
				windowAggregator.merge(pane, paneAcc);
			}
		}
	}

	@Override
	public void onWindowCleaning(W window) throws Exception {
		ctx.clearPreviousState(window);
	}
}
