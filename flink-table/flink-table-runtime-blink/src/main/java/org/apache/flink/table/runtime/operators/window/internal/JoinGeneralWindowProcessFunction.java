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
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView;

/**
 * The general implementation of {@link InternalWindowProcessFunction}
 * for window join.
 *
 * <p>The {@link WindowAssigner} should be a regular assigner without implement
 * {@code PanedWindowAssigner} or {@code MergingWindowAssigner}.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class JoinGeneralWindowProcessFunction<K, W extends Window>
		extends GeneralWindowProcessFunction<K, W>
		implements WindowJoinProcessFunction<K, W> {

	private static final long serialVersionUID = 5992545519395844485L;

	private final WindowJoinRecordStateView<W> view;

	public JoinGeneralWindowProcessFunction(
			WindowAssigner<W> windowAssigner,
			WindowJoinRecordStateView<W> view,
			long allowedLateness) {
		super(windowAssigner, allowedLateness);
		this.view = view;
	}

	@Override
	public Iterable<RowData> prepareInputsToJoin(W window) throws Exception {
		view.setCurrentNamespace(window);
		return view.getRecords();
	}
}
