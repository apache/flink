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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Sort based on and only on event-time.
 */
public class OnlyRowTimeSortOperator extends BaseTemporalSortOperator {

	private static final long serialVersionUID = 4040551859684667617L;

	private static final Logger LOG = LoggerFactory.getLogger(OnlyRowTimeSortOperator.class);

	private final BaseRowTypeInfo inputRowType;
	private final int rowTimeIdx;

	// State to collect rows between watermarks.
	private transient MapState<Long, List<BaseRow>> dataState;
	// State to keep the last triggering timestamp. Used to filter late events.
	private transient ValueState<Long> lastTriggeringTsState;

	public OnlyRowTimeSortOperator(BaseRowTypeInfo inputRowType, int rowTimeIdx) {
		this.inputRowType = inputRowType;
		this.rowTimeIdx = rowTimeIdx;
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening OnlyRowTimeSortOperator");

		BasicTypeInfo<Long> keyTypeInfo = BasicTypeInfo.LONG_TYPE_INFO;
		ListTypeInfo<BaseRow> valueTypeInfo = new ListTypeInfo<>(inputRowType);
		MapStateDescriptor<Long, List<BaseRow>> mapStateDescriptor = new MapStateDescriptor<>(
				"dataState", keyTypeInfo, valueTypeInfo);

		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		ValueStateDescriptor<Long> lastTriggeringTsDescriptor = new ValueStateDescriptor<>("lastTriggeringTsState",
				Long.class);
		lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);
	}

	@Override
	public void onEventTime(InternalTimer<BaseRow, VoidNamespace> timer) throws Exception {
		long timestamp = timer.getTimestamp();
		// gets all rows for the triggering timestamps
		dataState.get(timestamp).forEach((BaseRow row) -> collector.collect(row));

		// remove emitted rows from state
		dataState.remove(timestamp);
		lastTriggeringTsState.update(timestamp);
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow input = element.getValue();

		// timestamp of the processed row
		long rowTime = input.getLong(rowTimeIdx);

		Long lastTriggeringTs = lastTriggeringTsState.value();

		// check if the row is late and drop it if it is late
		if (lastTriggeringTs == null || rowTime > lastTriggeringTs) {
			// get list for timestamp
			List<BaseRow> rows = dataState.get(rowTime);
			if (null != rows) {
				rows.add(input);
				dataState.put(rowTime, rows);
			} else {
				List<BaseRow> newRows = new ArrayList<>();
				newRows.add(input);
				dataState.put(rowTime, newRows);

				// register event time timer
				timerService.registerEventTimeTimer(rowTime);
			}

		}
	}
}
