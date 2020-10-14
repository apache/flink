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

package org.apache.flink.table.runtime.operators.window.join.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import org.apache.commons.compress.utils.Lists;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility to create a  {@link WindowedStateView} for join depends on {@link JoinInputSideSpec}.
 */
public final class WindowJoinRecordStateViews {

	/**
	 * Creates a {@link JoinRecordStateView} depends on {@link JoinInputSideSpec}.
	 */
	public static <W extends Window> WindowJoinRecordStateView<W> create(
			WindowedStateView.Context ctx,
			TypeSerializer<W> windowSerializer,
			String stateName,
			JoinInputSideSpec inputSideSpec,
			InternalTypeInfo<RowData> recordType) throws Exception {
		if (inputSideSpec.joinKeyContainsUniqueKey()) {
			return new JoinKeyContainsUniqueKey<>(
					ctx,
					windowSerializer,
					stateName,
					recordType);
		} else if (inputSideSpec.hasUniqueKey()) {
			return new InputSideHasUniqueKey<>(
					ctx,
					windowSerializer,
					stateName,
					recordType,
					inputSideSpec.getUniqueKeyType(),
					inputSideSpec.getUniqueKeySelector());
		} else {
			return new InputSideHasNoUniqueKey<>(
					ctx,
					windowSerializer,
					stateName,
					recordType);
		}
	}

	// -------------------------------------------------------------------------
    //  Inner Classes
    // -------------------------------------------------------------------------

	/**
	 * View for join input whose join key are unique (contains unique key).
	 */
	private static final class JoinKeyContainsUniqueKey<W extends Window>
			implements WindowJoinRecordStateView<W> {

		private final InternalValueState<RowData, W, RowData> recordState;

		private JoinKeyContainsUniqueKey(
				WindowedStateView.Context ctx,
				TypeSerializer<W> windowSerializer,
				String stateName,
				InternalTypeInfo<RowData> recordType) throws Exception {
			ValueStateDescriptor<RowData> recordStateDesc = new ValueStateDescriptor<>(
					stateName,
					recordType);
			this.recordState = (InternalValueState<RowData, W, RowData>)
					ctx.getOrCreateKeyedState(windowSerializer, recordStateDesc);
		}

		@Override
		public void addRecord(RowData record) throws Exception {
			recordState.update(record);
		}

		@Override
		public void retractRecord(RowData record) {
			recordState.clear();
		}

		@Override
		public Iterable<RowData> getRecords() throws Exception {
			RowData record = recordState.value();
			if (record != null) {
				return Collections.singletonList(record);
			}
			return Collections.emptyList();
		}

		@Override
		public void setCurrentNamespace(W window) {
			recordState.setCurrentNamespace(window);
		}

		@Override
		public void clear() {
			recordState.clear();
		}

		@Override
		public void mergeNamespaces(W target, Iterable<W> sources) {
			throw new UnsupportedOperationException("Window merge should never happen because the join keys are unique");
		}
	}

	/**
	 * View for join input which are unique for each row by the unique key.
	 */
	private static final class InputSideHasUniqueKey<W extends Window>
			implements WindowJoinRecordStateView<W> {

		// stores record in the mapping <UK, Record>
		private final InternalMapState<RowData, W, RowData, RowData> recordState;
		private final KeySelector<RowData, RowData> uniqueKeySelector;

		private InputSideHasUniqueKey(
				WindowedStateView.Context ctx,
				TypeSerializer<W> windowSerializer,
				String stateName,
				InternalTypeInfo<RowData> recordType,
				InternalTypeInfo<RowData> uniqueKeyType,
				KeySelector<RowData, RowData> uniqueKeySelector) throws Exception {
			checkNotNull(uniqueKeyType);
			checkNotNull(uniqueKeySelector);
			MapStateDescriptor<RowData, RowData> recordStateDesc = new MapStateDescriptor<>(
				stateName,
				uniqueKeyType,
				recordType);
			this.recordState = (InternalMapState<RowData, W, RowData, RowData>)
					ctx.getOrCreateKeyedState(windowSerializer, recordStateDesc);
			this.uniqueKeySelector = uniqueKeySelector;
		}

		@Override
		public void addRecord(RowData record) throws Exception {
			RowData uniqueKey = uniqueKeySelector.getKey(record);
			recordState.put(uniqueKey, record);
		}

		@Override
		public void retractRecord(RowData record) throws Exception {
			RowData uniqueKey = uniqueKeySelector.getKey(record);
			recordState.remove(uniqueKey);
		}

		@Override
		public Iterable<RowData> getRecords() throws Exception {
			return recordState.values();
		}

		@Override
		public void setCurrentNamespace(W window) {
			recordState.setCurrentNamespace(window);
		}

		@Override
		public void clear() {
			recordState.clear();
		}

		@Override
		public void mergeNamespaces(W target, Iterable<W> sources) throws Exception {
			for (W window : sources) {
				recordState.setCurrentNamespace(window);
				Iterable<RowData> rows = recordState.values();
				recordState.setCurrentNamespace(target);
				for (RowData r : rows) {
					RowData uniqueKey = uniqueKeySelector.getKey(r);
					recordState.put(uniqueKey, r);
				}
			}
		}
	}

	/**
	 * View for join input which has duplicates within rows.
	 */
	private static final class InputSideHasNoUniqueKey<W extends Window>
			implements WindowJoinRecordStateView<W> {

		// stores record in the mapping <Record, duplicates>
		private final InternalMapState<RowData, W, RowData, Integer> recordState;

		private InputSideHasNoUniqueKey(
				WindowedStateView.Context ctx,
				TypeSerializer<W> windowSerializer,
				String stateName,
				InternalTypeInfo<RowData> recordType) throws Exception {
			MapStateDescriptor<RowData, Integer> recordStateDesc = new MapStateDescriptor<>(
				stateName,
				recordType,
				Types.INT);
			this.recordState = (InternalMapState<RowData, W, RowData, Integer>)
					ctx.getOrCreateKeyedState(windowSerializer, recordStateDesc);
		}

		@Override
		public void addRecord(RowData record) throws Exception {
			Integer cnt = recordState.get(record);
			if (cnt != null) {
				cnt += 1;
			} else {
				cnt = 1;
			}
			recordState.put(record, cnt);
		}

		@Override
		public void retractRecord(RowData record) throws Exception {
			Integer cnt = recordState.get(record);
			if (cnt != null) {
				if (cnt > 1) {
					recordState.put(record, cnt - 1);
				} else {
					recordState.remove(record);
				}
			}
			// ignore cnt == null, which means state may be expired
		}

		@Override
		public Iterable<RowData> getRecords() {
			return new IterableIterator<RowData>() {

				private Iterator<Map.Entry<RowData, Integer>> backingIterator;
				private List<Map.Entry<RowData, Integer>> backingRecords;
				private RowData record;
				private int remainingTimes = 0;

				@Override
				public boolean hasNext() {
					return backingIterator.hasNext() || remainingTimes > 0;
				}

				@Override
				public RowData next() {
					if (remainingTimes > 0) {
						remainingTimes--;
					} else {
						Map.Entry<RowData, Integer> entry = backingIterator.next();
						record = entry.getKey();
						remainingTimes = entry.getValue() - 1;
						if (remainingTimes > 0) {
							checkNotNull(record);
						}
					}
					return record;
				}

				@Nonnull
				@Override
				public Iterator<RowData> iterator() {
					try {
						reset();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					return this;
				}

				/** Reset the iterator so that it can be visited from the start every time. */
				private void reset() throws Exception {
					if (backingRecords == null) {
						backingRecords = Lists.newArrayList(recordState.entries().iterator());
					}
					backingIterator = backingRecords.iterator();
					this.remainingTimes = 0;
				}
			};
		}

		@Override
		public void setCurrentNamespace(W window) {
			recordState.setCurrentNamespace(window);
		}

		@Override
		public void clear() {
			recordState.clear();
		}

		@Override
		public void mergeNamespaces(W target, Iterable<W> sources) throws Exception {
			for (W window : sources) {
				recordState.setCurrentNamespace(window);
				Iterable<Map.Entry<RowData, Integer>> entries = recordState.entries();
				recordState.setCurrentNamespace(target);
				for (Map.Entry<RowData, Integer> entry : entries) {
					final RowData r = entry.getKey();
					if (recordState.contains(r)) {
						Integer newCnt = recordState.get(r) + entry.getValue();
						recordState.put(r, newCnt);
					} else {
						recordState.put(r, entry.getValue());
					}
				}
			}
		}
	}
}
