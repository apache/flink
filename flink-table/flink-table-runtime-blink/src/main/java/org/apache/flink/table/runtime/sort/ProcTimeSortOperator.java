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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.NormalizedKeyComputer;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Sort on proc-time and additional secondary sort attributes.
 */
public class ProcTimeSortOperator extends BaseTemporalSortOperator {

	private static final long serialVersionUID = -2028983921907321193L;

	private static final Logger LOG = LoggerFactory.getLogger(ProcTimeSortOperator.class);

	private final BaseRowTypeInfo inputRowType;
	private final long memorySize;

	private GeneratedNormalizedKeyComputer gComputer;
	private GeneratedRecordComparator gComparator;

	private transient ListState<BaseRow> dataState;

	private transient BinaryRowSerializer binarySerializer;

	private transient BinaryInMemorySortBuffer sortBuffer;
	private transient IndexedSorter sorter;

	private transient MemoryManager memManager;
	private transient List<MemorySegment> memorySegments;

	/**
	 * @param inputRowType The data type of the input data.
	 * @param memorySize The size of in memory buffer.
	 * @param gComputer generated NormalizedKeyComputer.
	 * @param gComparator generated comparator.
	 */
	public ProcTimeSortOperator(BaseRowTypeInfo inputRowType, long memorySize, GeneratedNormalizedKeyComputer gComputer,
			GeneratedRecordComparator gComparator) {
		this.inputRowType = inputRowType;
		this.memorySize = memorySize;
		this.gComputer = gComputer;
		this.gComparator = gComparator;
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening ProcTimeSortOperator");
		ExecutionConfig executionConfig = getExecutionConfig();
		BaseRowSerializer inputSerializer = inputRowType.createSerializer(executionConfig);
		binarySerializer = new BinaryRowSerializer(inputSerializer.getArity());
		ClassLoader cl = getContainingTask().getUserCodeClassLoader();
		NormalizedKeyComputer computer = gComputer.newInstance(cl);
		RecordComparator comparator = gComparator.newInstance(cl);
		gComputer = null;
		gComparator = null;

		memManager = getContainingTask().getEnvironment().getMemoryManager();
		memorySegments = memManager.allocatePages(getContainingTask(), (int) (memorySize / memManager.getPageSize()));
		sortBuffer = BinaryInMemorySortBuffer.createBuffer(computer, inputSerializer, binarySerializer,
				comparator, memorySegments);
		sorter = new QuickSort();

		ListStateDescriptor<BaseRow> sortDescriptor = new ListStateDescriptor<>(
				"sortState", inputRowType);

		dataState = getRuntimeContext().getListState(sortDescriptor);
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow input = element.getValue();
		long currentTime = timerService.currentProcessingTime();

		// buffer the event incoming event
		dataState.add(input);

		// register a timer for the next millisecond to sort and emit buffered data
		timerService.registerProcessingTimeTimer(currentTime + 1);
	}

	@Override
	public void onProcessingTime(InternalTimer<BaseRow, VoidNamespace> timer) throws Exception {

		// gets all rows for the triggering timestamps
		dataState.get().forEach((BaseRow row) -> {
			try {
				if (!sortBuffer.write(row)) {
					throw new RuntimeException(
							new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
									+ sortBuffer.getCapacity() + " bytes)."));
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		sorter.sort(sortBuffer);

		// emit the sorted inputs
		BinaryRow row = binarySerializer.createInstance();
		MutableObjectIterator<BinaryRow> iterator = sortBuffer.getIterator();
		while ((row = iterator.next(row)) != null) {
			collector.collect(row);
		}

		// remove all buffered rows
		sortBuffer.reset();

		// remove emitted rows from state
		dataState.clear();
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing ProcTimeSortOperator");
		super.close();
		if (sortBuffer != null) {
			sortBuffer.dispose();
		}
		memManager.release(memorySegments);
	}
}
