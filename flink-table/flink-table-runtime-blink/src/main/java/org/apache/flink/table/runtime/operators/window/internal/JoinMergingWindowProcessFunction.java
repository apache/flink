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
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView;

import java.util.Collection;

/**
 * The implementation of {@link InternalWindowProcessFunction} for
 * window aggregate {@link MergingWindowAssigner}.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class JoinMergingWindowProcessFunction<K, W extends Window>
		extends MergingWindowProcessFunction<K, W>
		implements WindowJoinProcessFunction<K, W> {

	private static final long serialVersionUID = 1L;

	private final WindowJoinRecordStateView<W> view;

	public JoinMergingWindowProcessFunction(
			MergingWindowAssigner<W> windowAssigner,
			String mergingWindowStateName,
			WindowJoinRecordStateView<W> view,
			TypeSerializer<W> windowSerializer,
			long allowedLateness) {
		super(mergingWindowStateName, windowAssigner, windowSerializer, allowedLateness);
		this.view = view;
	}

	@Override
	public Iterable<RowData> prepareInputsToJoin(W window) throws Exception {
		W stateWindow = mergingWindows.getStateWindow(window);
		this.view.setCurrentNamespace(stateWindow);
		return this.view.getRecords();
	}

	@Override
	protected void mergeNamespaces(W stateWindowResult, Collection<W> stateWindowsToBeMerged)
			throws Exception {
		if (!stateWindowsToBeMerged.isEmpty()) {
			this.view.mergeNamespaces(stateWindowResult, stateWindowsToBeMerged);
			for (W w : stateWindowsToBeMerged) {
				// clear merged window
				this.view.setCurrentNamespace(w);
				this.view.clear();
			}
			this.view.setCurrentNamespace(stateWindowResult);
		}
	}
}
