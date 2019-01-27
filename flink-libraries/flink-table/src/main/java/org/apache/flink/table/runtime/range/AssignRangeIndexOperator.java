/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.range;

import org.apache.flink.api.common.distributions.RangeBoundaries;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

/**
 * This two-input-operator require a DataStream with RangeBoundaries as broadcast input, and
 * generate Tuple2 which includes range index and record from the other input itself as output.
 *
 */
public class AssignRangeIndexOperator extends AbstractStreamOperatorWithMetrics<Tuple2<Integer, BaseRow>>
		implements TwoInputStreamOperator<Object[][], BaseRow, Tuple2<Integer, BaseRow>> {

	private final KeyExtractor keyExtractor;

	private transient RangeBoundaries rangeBoundaries;
	private transient Collector<Tuple2<Integer, BaseRow>> collector;

	public AssignRangeIndexOperator(KeyExtractor keyExtractor) {
		this.keyExtractor = keyExtractor;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.collector = new StreamRecordCollector<>(output);
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.FIRST;
	}

	@Override
	public TwoInputSelection processElement1(
			StreamRecord<Object[][]> streamRecord) throws Exception {
		rangeBoundaries = new CommonRangeBoundaries(keyExtractor, streamRecord.getValue());
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement2(
			StreamRecord<BaseRow> streamRecord) throws Exception {
		if (rangeBoundaries == null) {
			throw new RuntimeException("There should be one data from the first input.");
		}
		Tuple2<Integer, BaseRow> tupleWithPartitionId = new Tuple2<>();
		tupleWithPartitionId.f0 = rangeBoundaries.getRangeIndex(streamRecord.getValue());
		tupleWithPartitionId.f1 = streamRecord.getValue();
		collector.collect(tupleWithPartitionId);
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput1() throws Exception {

	}

	@Override
	public void endInput2() throws Exception {

	}
}
