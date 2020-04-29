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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Sort on proc-time and additional secondary sort attributes.
 */
public class ProcTimeSortOperator extends BaseTemporalSortOperator {

	private static final long serialVersionUID = -2028983921907321193L;

	private static final Logger LOG = LoggerFactory.getLogger(ProcTimeSortOperator.class);

	private final RowDataTypeInfo inputRowType;

	private GeneratedRecordComparator gComparator;
	private transient RecordComparator comparator;
	private transient List<RowData> sortBuffer;

	private transient ListState<RowData> dataState;

	/**
	 * @param inputRowType The data type of the input data.
	 * @param gComparator generated comparator.
	 */
	public ProcTimeSortOperator(RowDataTypeInfo inputRowType, GeneratedRecordComparator gComparator) {
		this.inputRowType = inputRowType;
		this.gComparator = gComparator;
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening ProcTimeSortOperator");

		comparator = gComparator.newInstance(getContainingTask().getUserCodeClassLoader());
		gComparator = null;
		sortBuffer = new ArrayList<>();

		ListStateDescriptor<RowData> sortDescriptor = new ListStateDescriptor<>("sortState", inputRowType);
		dataState = getRuntimeContext().getListState(sortDescriptor);
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		RowData input = element.getValue();
		long currentTime = timerService.currentProcessingTime();

		// buffer the event incoming event
		dataState.add(input);

		// register a timer for the next millisecond to sort and emit buffered data
		timerService.registerProcessingTimeTimer(currentTime + 1);
	}

	@Override
	public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {

		// gets all rows for the triggering timestamps
		Iterable<RowData> inputs = dataState.get();

		// insert all rows into the sort buffer
		sortBuffer.clear();
		inputs.forEach(sortBuffer::add);

		// sort the rows
		sortBuffer.sort(comparator);

		// Emit the rows in order
		sortBuffer.forEach((RowData row) -> collector.collect(row));

		// remove all buffered rows
		dataState.clear();
	}

	@Override
	public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
		throw new UnsupportedOperationException("Now Sort only is supported based processing time here!");
	}

}
