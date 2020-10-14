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
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.util.Preconditions;

/**
 * The general implementation of {@link InternalWindowProcessFunction}
 * for window aggregate.
 *
 * <p>The {@link WindowAssigner} should be a regular assigner without implement
 * {@code PanedWindowAssigner} or {@code MergingWindowAssigner}.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class AggregateGeneralWindowProcessFunction<K, W extends Window>
		extends GeneralWindowProcessFunction<K, W>
		implements WindowAggregateProcessFunction<K, W>, WindowCleanAware<W> {

	private static final long serialVersionUID = 5992545519395844485L;

	private final NamespaceAggsHandleFunctionBase<W> windowAggregator;
	// A typed copy to have aggregate specific operations.
	protected WindowAggregateProcessFunction.Context<K, W> ctx;

	public AggregateGeneralWindowProcessFunction(
			WindowAssigner<W> windowAssigner,
			NamespaceAggsHandleFunctionBase<W> windowAggregator,
			long allowedLateness) {
		super(windowAssigner, allowedLateness);
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
		RowData acc = ctx.getWindowAccumulators(window);
		if (acc == null) {
			acc = windowAggregator.createAccumulators();
		}
		windowAggregator.setAccumulators(window, acc);
	}

	@Override
	public void onWindowCleaning(W window) throws Exception {
		ctx.clearPreviousState(window);
	}
}
