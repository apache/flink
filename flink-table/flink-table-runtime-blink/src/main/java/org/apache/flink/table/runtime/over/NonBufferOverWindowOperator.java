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

package org.apache.flink.table.runtime.over;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.generated.AggsHandleFunction;
import org.apache.flink.table.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;

/**
 * The operator for OVER window don't need cache data.
 * Then this operator can calculate the accumulator by the current row.
 *
 * <p>Some over windows do not need to buffer data, such as {@code rows between unbounded preceding and 0},
 * rank, etc. We introduce {@link NonBufferOverWindowOperator} to reduce the overhead of data copy in buffer.
 *
 * <p>NOTE: Use {@link NonBufferOverWindowOperator} only when all frames do not need buffer data.
 */
public class NonBufferOverWindowOperator extends TableStreamOperator<BaseRow>
		implements OneInputStreamOperator<BaseRow, BaseRow> {

	private GeneratedAggsHandleFunction[] aggsHandlers;
	private GeneratedRecordComparator genComparator;
	private final boolean[] resetAccumulators;

	private RecordComparator partitionComparator;
	private BaseRow lastInput;
	private AggsHandleFunction[] processors;
	private JoinedRow[] joinedRows;
	private StreamRecordCollector<BaseRow> collector;
	private AbstractRowSerializer<BaseRow> serializer;

	public NonBufferOverWindowOperator(
			GeneratedAggsHandleFunction[] aggsHandlers,
			GeneratedRecordComparator genComparator,
			boolean[] resetAccumulators) {
		this.aggsHandlers = aggsHandlers;
		this.genComparator = genComparator;
		this.resetAccumulators = resetAccumulators;
	}

	@Override
	public void open() throws Exception {
		super.open();

		ClassLoader cl = getUserCodeClassloader();
		serializer = (AbstractRowSerializer) getOperatorConfig().getTypeSerializerIn1(cl);
		partitionComparator = genComparator.newInstance(cl);
		genComparator = null;

		collector = new StreamRecordCollector<>(output);
		processors = new AggsHandleFunction[aggsHandlers.length];
		joinedRows = new JoinedRow[aggsHandlers.length];
		for (int i = 0; i < aggsHandlers.length; i++) {
			AggsHandleFunction func = aggsHandlers[i].newInstance(cl);
			func.open(new PerKeyStateDataViewStore(getRuntimeContext()));
			processors[i] = func;
			joinedRows[i] = new JoinedRow();
		}
		aggsHandlers = null;
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow input = element.getValue();
		boolean changePartition = lastInput == null || partitionComparator.compare(lastInput, input) != 0;

		//calculate the ACC
		BaseRow output = input;
		for (int i = 0; i < processors.length; i++) {
			AggsHandleFunction processor = processors[i];

			if (changePartition || resetAccumulators[i]) {
				processor.setAccumulators(processor.createAccumulators());
			}

			// TODO Reform AggsHandleFunction.getValue instead of use JoinedRow. Multilayer JoinedRow is slow.
			processor.accumulate(input);
			BaseRow value = processor.getValue();
			output = joinedRows[i].replace(output, value);
		}
		collector.collect(output);

		if (changePartition) {
			lastInput = serializer.copy(input);
		}
	}
}
