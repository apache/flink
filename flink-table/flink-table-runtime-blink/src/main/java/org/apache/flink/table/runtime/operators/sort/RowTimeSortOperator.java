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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Sort based on event-time and possibly additional secondary sort attributes.
 */
public class RowTimeSortOperator extends BaseTemporalSortOperator {

	private static final long serialVersionUID = 2085278292749212811L;

	private static final Logger LOG = LoggerFactory.getLogger(RowTimeSortOperator.class);

	private final RowDataTypeInfo inputRowType;
	private final int rowTimeIdx;

	private GeneratedRecordComparator gComparator;
	private transient RecordComparator comparator;

	// State to collect rows between watermarks.
	private transient MapState<Long, List<RowData>> dataState;
	// State to keep the last triggering timestamp. Used to filter late events.
	private transient ValueState<Long> lastTriggeringTsState;

	/**
	 * @param inputRowType The data type of the input data.
	 * @param rowTimeIdx The index of the rowTime field.
	 * @param gComparator generated comparator, could be null if only sort on RowTime field
	 */
	public RowTimeSortOperator(RowDataTypeInfo inputRowType, int rowTimeIdx, GeneratedRecordComparator gComparator) {
		this.inputRowType = inputRowType;
		Preconditions.checkArgument(rowTimeIdx >= 0 && rowTimeIdx < inputRowType.getArity(),
				"RowTimeIdx must be 0 or positive number and smaller than input row arity!");
		this.rowTimeIdx = rowTimeIdx;
		this.gComparator = gComparator;
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening RowTimeSortOperator");
		if (gComparator != null) {
			comparator = gComparator.newInstance(getContainingTask().getUserCodeClassLoader());
			gComparator = null;
		}

		BasicTypeInfo<Long> keyTypeInfo = BasicTypeInfo.LONG_TYPE_INFO;
		ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
		MapStateDescriptor<Long, List<RowData>> mapStateDescriptor = new MapStateDescriptor<>(
				"dataState", keyTypeInfo, valueTypeInfo);
		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		ValueStateDescriptor<Long> lastTriggeringTsDescriptor = new ValueStateDescriptor<>("lastTriggeringTsState",
				Long.class);
		lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		RowData input = element.getValue();

		// timestamp of the processed row
		long rowTime = input.getLong(rowTimeIdx);

		Long lastTriggeringTs = lastTriggeringTsState.value();

		// check if the row is late and drop it if it is late
		if (lastTriggeringTs == null || rowTime > lastTriggeringTs) {
			// get list for timestamp
			List<RowData> rows = dataState.get(rowTime);
			if (null != rows) {
				rows.add(input);
				dataState.put(rowTime, rows);
			} else {
				List<RowData> newRows = new ArrayList<>();
				newRows.add(input);
				dataState.put(rowTime, newRows);

				// register event time timer
				timerService.registerEventTimeTimer(rowTime);
			}

		}
	}

	@Override
	public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
		long timestamp = timer.getTimestamp();

		// gets all rows for the triggering timestamps
		List<RowData> inputs = dataState.get(timestamp);
		if (inputs != null) {
			// sort rows on secondary fields if necessary
			if (comparator != null) {
				inputs.sort(comparator);
			}

			// emit rows in order
			inputs.forEach((RowData row) -> collector.collect(row));

			// remove emitted rows from state
			dataState.remove(timestamp);
			lastTriggeringTsState.update(timestamp);
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
		throw new UnsupportedOperationException("Now Sort only is supported based event time here!");
	}

}
