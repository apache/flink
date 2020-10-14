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
import org.apache.flink.table.runtime.operators.window.assigners.PanedWindowAssigner;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

/**
 * The implementation of {@link InternalWindowProcessFunction} for
 * window aggregate {@link PanedWindowAssigner}.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class JoinPanedWindowProcessFunction<K, W extends Window>
		extends PanedWindowProcessFunction<K, W>
		implements WindowJoinProcessFunction<K, W> {

	private static final long serialVersionUID = 4259335376102569987L;

	private final WindowJoinRecordStateView<W> view;
	private final PanedWindowAssigner<W> windowAssigner;

	public JoinPanedWindowProcessFunction(
			PanedWindowAssigner<W> windowAssigner,
			WindowJoinRecordStateView<W> view,
			long allowedLateness) {
		super(windowAssigner, allowedLateness);
		this.windowAssigner = windowAssigner;
		this.view = view;
	}

	public Iterable<RowData> prepareInputsToJoin(W window) throws Exception {
		Iterable<W> panes = windowAssigner.splitIntoPanes(window);
		List<Iterable<RowData>> recordsIterables = new ArrayList<>();
		while (panes.iterator().hasNext()) {
			this.view.setCurrentNamespace(panes.iterator().next());
			Iterable<RowData> records = this.view.getRecords();
			if (records.iterator().hasNext()) {
				recordsIterables.add(records);
			}
		}
		return Iterables.concat(recordsIterables);
	}
}
