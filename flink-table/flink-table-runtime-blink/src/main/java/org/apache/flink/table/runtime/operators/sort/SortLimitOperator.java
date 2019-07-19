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

import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Operator for batch sort limit.
 */
public class SortLimitOperator extends TableStreamOperator<BaseRow>
		implements OneInputStreamOperator<BaseRow, BaseRow>, BoundedOneInput {

	private final boolean isGlobal;
	private final long limitStart;
	private final long limitEnd;
	private GeneratedRecordComparator genComparator;

	private transient PriorityQueue<BaseRow> heap;
	private transient Collector<BaseRow> collector;
	private transient RecordComparator comparator;
	private transient AbstractRowSerializer<BaseRow> inputSer;

	public SortLimitOperator(
			boolean isGlobal, long limitStart, long limitEnd, GeneratedRecordComparator genComparator) {
		this.isGlobal = isGlobal;
		this.limitStart = limitStart;
		this.limitEnd = limitEnd;
		this.genComparator = genComparator;
	}

	@Override
	public void open() throws Exception {
		super.open();

		inputSer = (AbstractRowSerializer) getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
		comparator = genComparator.newInstance(getUserCodeClassloader());
		genComparator = null;

		// reverse the comparision.
		heap = new PriorityQueue<>((int) limitEnd, (o1, o2) -> comparator.compare(o2, o1));
		this.collector = new StreamRecordCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow record = element.getValue();

		// Need copy element, because we will store record in heap.
		if (heap.size() >= limitEnd) {
			BaseRow peek = heap.peek();
			if (comparator.compare(peek, record) > 0) {
				heap.poll();
				heap.add(inputSer.copy(record));
			} // else fail, this record don't need insert to the heap.
		} else {
			heap.add(inputSer.copy(record));
		}
	}

	@Override
	public void endInput() throws Exception {
		if (isGlobal) {
			// Global sort, we need sort the results and pick records in limitStart to limitEnd.
			List<BaseRow> list = new ArrayList<>(heap);
			list.sort((o1, o2) -> comparator.compare(o1, o2));

			int maxIndex = (int) Math.min(limitEnd, list.size());
			for (int i = (int) limitStart; i < maxIndex; i++) {
				collector.collect(list.get(i));
			}
		} else {
			for (BaseRow row : heap) {
				collector.collect(row);
			}
		}
	}
}
