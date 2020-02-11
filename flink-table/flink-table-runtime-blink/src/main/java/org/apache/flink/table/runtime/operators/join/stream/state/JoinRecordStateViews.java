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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
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
			BaseRowTypeInfo recordType,
			long retentionTime,
			boolean stateCleaningEnabled) {
		StateTtlConfig ttlConfig = createTtlConfig(retentionTime, stateCleaningEnabled);
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

	static StateTtlConfig createTtlConfig(long retentionTime, boolean stateCleaningEnabled) {
		if (stateCleaningEnabled) {
			checkArgument(retentionTime > 0);
			return StateTtlConfig
				.newBuilder(Time.milliseconds(retentionTime))
				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
				.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
				.cleanupInBackground()
				.build();
		} else {
			return StateTtlConfig.DISABLED;
		}
	}

	// ------------------------------------------------------------------------------------

	private static final class JoinKeyContainsUniqueKey implements JoinRecordStateView {

		private final ValueState<BaseRow> recordState;
		private final List<BaseRow> reusedList;

		private JoinKeyContainsUniqueKey(
				RuntimeContext ctx,
				String stateName,
				BaseRowTypeInfo recordType,
				StateTtlConfig ttlConfig) {
			ValueStateDescriptor<BaseRow> recordStateDesc = new ValueStateDescriptor<>(
				stateName,
				recordType);
			if (!ttlConfig.equals(StateTtlConfig.DISABLED)) {
				recordStateDesc.enableTimeToLive(ttlConfig);
			}
			this.recordState = ctx.getState(recordStateDesc);
			// the result records always not more than 1
			this.reusedList = new ArrayList<>(1);
		}

		@Override
		public void addRecord(BaseRow record) throws Exception {
			recordState.update(record);
		}

		@Override
		public void retractRecord(BaseRow record) throws Exception {
			recordState.clear();
		}

		@Override
		public Iterable<BaseRow> getRecords() throws Exception {
			reusedList.clear();
			BaseRow record = recordState.value();
			if (record != null) {
				reusedList.add(record);
			}
			return reusedList;
		}
	}

	private static final class InputSideHasUniqueKey implements JoinRecordStateView {

		// stores record in the mapping <UK, Record>
		private final MapState<BaseRow, BaseRow> recordState;
		private final KeySelector<BaseRow, BaseRow> uniqueKeySelector;

		private InputSideHasUniqueKey(
				RuntimeContext ctx,
				String stateName,
				BaseRowTypeInfo recordType,
				BaseRowTypeInfo uniqueKeyType,
				KeySelector<BaseRow, BaseRow> uniqueKeySelector,
				StateTtlConfig ttlConfig) {
			checkNotNull(uniqueKeyType);
			checkNotNull(uniqueKeySelector);
			MapStateDescriptor<BaseRow, BaseRow> recordStateDesc = new MapStateDescriptor<>(
				stateName,
				uniqueKeyType,
				recordType);
			if (!ttlConfig.equals(StateTtlConfig.DISABLED)) {
				recordStateDesc.enableTimeToLive(ttlConfig);
			}
			this.recordState = ctx.getMapState(recordStateDesc);
			this.uniqueKeySelector = uniqueKeySelector;
		}

		@Override
		public void addRecord(BaseRow record) throws Exception {
			BaseRow uniqueKey = uniqueKeySelector.getKey(record);
			recordState.put(uniqueKey, record);
		}

		@Override
		public void retractRecord(BaseRow record) throws Exception {
			BaseRow uniqueKey = uniqueKeySelector.getKey(record);
			recordState.remove(uniqueKey);
		}

		@Override
		public Iterable<BaseRow> getRecords() throws Exception {
			return recordState.values();
		}
	}

	private static final class InputSideHasNoUniqueKey implements JoinRecordStateView {

		private final MapState<BaseRow, Integer> recordState;

		private InputSideHasNoUniqueKey(
				RuntimeContext ctx,
				String stateName,
				BaseRowTypeInfo recordType,
				StateTtlConfig ttlConfig) {
			MapStateDescriptor<BaseRow, Integer> recordStateDesc = new MapStateDescriptor<>(
				stateName,
				recordType,
				Types.INT);
			if (!ttlConfig.equals(StateTtlConfig.DISABLED)) {
				recordStateDesc.enableTimeToLive(ttlConfig);
			}
			this.recordState = ctx.getMapState(recordStateDesc);
		}

		@Override
		public void addRecord(BaseRow record) throws Exception {
			Integer cnt = recordState.get(record);
			if (cnt != null) {
				cnt += 1;
			} else {
				cnt = 1;
			}
			recordState.put(record, cnt);
		}

		@Override
		public void retractRecord(BaseRow record) throws Exception {
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
		public Iterable<BaseRow> getRecords() throws Exception {
			return new IterableIterator<BaseRow>() {

				private final Iterator<Map.Entry<BaseRow, Integer>> backingIterable = recordState.entries().iterator();
				private BaseRow record;
				private int remainingTimes = 0;

				@Override
				public boolean hasNext() {
					return backingIterable.hasNext() || remainingTimes > 0;
				}

				@Override
				public BaseRow next() {
					if (remainingTimes > 0) {
						checkNotNull(record);
						remainingTimes--;
						return record;
					} else {
						Map.Entry<BaseRow, Integer> entry = backingIterable.next();
						record = entry.getKey();
						remainingTimes = entry.getValue() - 1;
						return record;
					}
				}

				@Override
				public Iterator<BaseRow> iterator() {
					return this;
				}
			};
		}
	}
}
