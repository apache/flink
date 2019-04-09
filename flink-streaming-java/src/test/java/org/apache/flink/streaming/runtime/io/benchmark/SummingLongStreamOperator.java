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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for operators of summing longs.
 */
public class SummingLongStreamOperator
	extends AbstractStreamOperator<Long> implements TwoInputStreamOperator<Long, Long, Long>, InputSelectable {

	protected InputSelection inputSelection = InputSelection.ALL;

	protected AbstractTaskInputProcessorThread processorThread;

	protected long totalRecordsPerIteration;

	protected long numRecords1PerIteration;
	protected long numRecords2PerIteration;

	protected long numRecords1 = 0;
	protected long numRecords2 = 0;

	private long sum1 = 0;
	private long sum2 = 0;

	private long currentIterationRecords = 0;

	public void open(AbstractTaskInputProcessorThread processorThread, long numRecords1PerIteration, long numRecords2PerIteration) {
		this.processorThread = checkNotNull(processorThread);

		this.numRecords1PerIteration = numRecords1PerIteration;
		this.numRecords2PerIteration = numRecords2PerIteration;
		this.totalRecordsPerIteration = numRecords1PerIteration + numRecords2PerIteration;
	}

	@Override
	public void processElement1(StreamRecord<Long> element) throws Exception {
		numRecords1++;
		sum1 += element.getValue();

		incIterationRecordsAndCheckToEndIteration();
	}

	@Override
	public void processElement2(StreamRecord<Long> element) throws Exception {
		numRecords2++;
		sum2 += element.getValue();

		incIterationRecordsAndCheckToEndIteration();
	}

	@Override
	public InputSelection nextSelection() {
		return inputSelection;
	}

	public long getRecordNumber1() {
		return numRecords1;
	}

	public long getRecordNumber2() {
		return numRecords2;
	}

	public long getSum1() {
		return sum1;
	}

	public long getSum2() {
		return sum2;
	}

	private void incIterationRecordsAndCheckToEndIteration() {
		currentIterationRecords++;
		if (currentIterationRecords == totalRecordsPerIteration) {
			// end the iteration
			processorThread.endIteration();

			currentIterationRecords = 0;
		}
	}
}
