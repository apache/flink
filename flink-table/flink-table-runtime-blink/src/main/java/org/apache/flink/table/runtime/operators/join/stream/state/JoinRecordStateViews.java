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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility to create a {@link JoinRecordStateView} depends on {@link JoinInputSideSpec}.
 */
public final class JoinRecordStateViews {

	/**
	 * Creates a {@link JoinRecordStateView} depends on {@link JoinInputSideSpec}.
	 */
	public static JoinRecordStateView create(
			RuntimeContext ctx,
			String stateName,
			JoinInputSideSpec inputSideSpec,
			RowDataTypeInfo recordType,
			long retentionTime) {
		StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
		if (inputSideSpec.hasUniqueKey()) {
			if (inputSideSpec.joinKeyContainsUniqueKey()) {
				return new JoinKeyContainsUniqueKey(ctx, stateName, recordType, ttlConfig);
			} else {
				return new InputSideHasUniqueKey(
					ctx,
					stateName,
					recordType,
					inputSideSpec.getUniqueKeyType(),
					inputSideSpec.getUniqueKeySelector(),
					ttlConfig);
			}
		} else {
			return new InputSideHasNoUniqueKey(ctx, stateName, recordType, ttlConfig);
		}
	}

	// ------------------------------------------------------------------------------------

	private static final class JoinKeyContainsUniqueKey implements JoinRecordStateView {

		private final ValueState<RowData> recordState;
		private final List<RowData> reusedList;

		private JoinKeyContainsUniqueKey(
				RuntimeContext ctx,
				String stateName,
				RowDataTypeInfo recordType,
				StateTtlConfig ttlConfig) {
			ValueStateDescriptor<RowData> recordStateDesc = new ValueStateDescriptor<>(
				stateName,
				recordType);
			if (ttlConfig.isEnabled()) {
				recordStateDesc.enableTimeToLive(ttlConfig);
			}
			this.recordState = ctx.getState(recordStateDesc);
			// the result records always not more than 1
			this.reusedList = new ArrayList<>(1);
		}

		@Override
		public void addRecord(RowData record) throws Exception {
			recordState.update(record);
		}

		@Override
		public void retractRecord(RowData record) throws Exception {
			recordState.clear();
		}

		@Override
		public Iterable<RowData> getRecords() throws Exception {
			reusedList.clear();
			RowData record = recordState.value();
			if (record != null) {
				reusedList.add(record);
			}
			return reusedList;
		}
	}

	private static final class InputSideHasUniqueKey implements JoinRecordStateView {

		// stores record in the mapping <UK, Record>
		private final MapState<RowData, RowData> recordState;
		private final KeySelector<RowData, RowData> uniqueKeySelector;

		private InputSideHasUniqueKey(
				RuntimeContext ctx,
				String stateName,
				RowDataTypeInfo recordType,
				RowDataTypeInfo uniqueKeyType,
				KeySelector<RowData, RowData> uniqueKeySelector,
				StateTtlConfig ttlConfig) {
			checkNotNull(uniqueKeyType);
			checkNotNull(uniqueKeySelector);
			MapStateDescriptor<RowData, RowData> recordStateDesc = new MapStateDescriptor<>(
				stateName,
				uniqueKeyType,
				recordType);
			if (ttlConfig.isEnabled()) {
				recordStateDesc.enableTimeToLive(ttlConfig);
			}
			this.recordState = ctx.getMapState(recordStateDesc);
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
	}

	private static final class InputSideHasNoUniqueKey implements JoinRecordStateView {

		private final MapState<RowData, Integer> recordState;

		private InputSideHasNoUniqueKey(
				RuntimeContext ctx,
				String stateName,
				RowDataTypeInfo recordType,
				StateTtlConfig ttlConfig) {
			MapStateDescriptor<RowData, Integer> recordStateDesc = new MapStateDescriptor<>(
				stateName,
				recordType,
				Types.INT);
			if (ttlConfig.isEnabled()) {
				recordStateDesc.enableTimeToLive(ttlConfig);
			}
			this.recordState = ctx.getMapState(recordStateDesc);
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
		public Iterable<RowData> getRecords() throws Exception {
			return new IterableIterator<RowData>() {

				private final Iterator<Map.Entry<RowData, Integer>> backingIterable = recordState.entries().iterator();
				private RowData record;
				private int remainingTimes = 0;

				@Override
				public boolean hasNext() {
					return backingIterable.hasNext() || remainingTimes > 0;
				}

				@Override
				public RowData next() {
					if (remainingTimes > 0) {
						checkNotNull(record);
						remainingTimes--;
						return record;
					} else {
						Map.Entry<RowData, Integer> entry = backingIterable.next();
						record = entry.getKey();
						remainingTimes = entry.getValue() - 1;
						return record;
					}
				}

				@Override
				public Iterator<RowData> iterator() {
					return this;
				}
			};
		}
	}
}
