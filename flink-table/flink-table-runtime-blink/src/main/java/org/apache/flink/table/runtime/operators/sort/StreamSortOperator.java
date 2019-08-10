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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Operator for stream sort.
 */
public class StreamSortOperator extends TableStreamOperator<BaseRow> implements
		OneInputStreamOperator<BaseRow, BaseRow> {

	private static final long serialVersionUID = 9042068324817807379L;

	private static final Logger LOG = LoggerFactory.getLogger(StreamSortOperator.class);

	private final BaseRowTypeInfo inputRowType;
	private GeneratedRecordComparator gComparator;
	private transient RecordComparator comparator;
	private transient BaseRowSerializer baseRowSerializer;
	private transient StreamRecordCollector<BaseRow> collector;

	private transient ListState<Tuple2<BaseRow, Long>> bufferState;

	// inputBuffer buffers all input elements, key is BaseRow, value is appear times.
	private transient HashMap<BaseRow, Long> inputBuffer;

	public StreamSortOperator(BaseRowTypeInfo inputRowType, GeneratedRecordComparator gComparator) {
		this.inputRowType = inputRowType;
		this.gComparator = gComparator;
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening StreamSortOperator");
		ExecutionConfig executionConfig = getExecutionConfig();
		this.baseRowSerializer = inputRowType.createSerializer(executionConfig);

		comparator = gComparator.newInstance(getContainingTask().getUserCodeClassLoader());
		gComparator = null;

		this.collector = new StreamRecordCollector<>(output);
		this.inputBuffer = new HashMap<>();

		// restore state
		if (bufferState != null) {
			bufferState.get().forEach((Tuple2<BaseRow, Long> input) -> inputBuffer.put(input.f0, input.f1));
		}
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow originalInput = element.getValue();
		BinaryRow input = baseRowSerializer.toBinaryRow(originalInput).copy();
		BaseRowUtil.setAccumulate(input);
		long count = inputBuffer.getOrDefault(input, 0L);
		if (BaseRowUtil.isAccumulateMsg(originalInput)) {
			inputBuffer.put(input, count + 1);
		} else {
			if (count == 0L) {
				throw new RuntimeException("BaseRow not exist!");
			} else if (count == 1) {
				inputBuffer.remove(input);
			} else {
				inputBuffer.put(input, count - 1);
			}
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		TupleTypeInfo<Tuple2<BaseRow, Long>> tupleType = new TupleTypeInfo<>(inputRowType, Types.LONG);
		this.bufferState = context.getOperatorStateStore()
				.getListState(new ListStateDescriptor<>("localBufferState", tupleType));
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		// clear state first
		bufferState.clear();

		List<Tuple2<BaseRow, Long>> dataToFlush = new ArrayList<>(inputBuffer.size());
		inputBuffer.forEach((key, value) -> dataToFlush.add(Tuple2.of(key, value)));

		// batch put
		bufferState.addAll(dataToFlush);
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing StreamSortOperator");

		// BoundedOneInput can not coexistence with checkpoint, so we emit output in close.
		if (!inputBuffer.isEmpty()) {
			List<BaseRow> rowsSet = new ArrayList<>();
			rowsSet.addAll(inputBuffer.keySet());
			// sort the rows
			rowsSet.sort(comparator);

			// Emit the rows in order
			rowsSet.forEach((BaseRow row) -> {
				long count = inputBuffer.get(row);
				for (int i = 1; i <= count; i++) {
					collector.collect(row);
				}
			});
		}
		super.close();
	}
}
