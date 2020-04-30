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


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.hash.NonReusingBuildFirstReOpenableHashJoinIterator;
import org.apache.flink.runtime.operators.hash.NonReusingBuildSecondReOpenableHashJoinIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildFirstReOpenableHashJoinIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildSecondReOpenableHashJoinIterator;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public abstract class AbstractCachedBuildSideJoinDriver<IT1, IT2, OT> extends JoinDriver<IT1, IT2, OT> implements ResettableDriver<FlatJoinFunction<IT1, IT2, OT>, OT> {

	private volatile JoinTaskIterator<IT1, IT2, OT> matchIterator;
	
	private final int buildSideIndex;
	
	private final int probeSideIndex;

	private boolean objectReuseEnabled = false;


	protected AbstractCachedBuildSideJoinDriver(int buildSideIndex, int probeSideIndex) {
		this.buildSideIndex = buildSideIndex;
		this.probeSideIndex = probeSideIndex;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean isInputResettable(int inputNum) {
		if (inputNum < 0 || inputNum > 1) {
			throw new IndexOutOfBoundsException();
		}
		return inputNum == buildSideIndex;
	}

	@Override
	public void initialize() throws Exception {
		TaskConfig config = this.taskContext.getTaskConfig();

		final Counter numRecordsIn = taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
		
		TypeSerializer<IT1> serializer1 = this.taskContext.<IT1>getInputSerializer(0).getSerializer();
		TypeSerializer<IT2> serializer2 = this.taskContext.<IT2>getInputSerializer(1).getSerializer();
		TypeComparator<IT1> comparator1 = this.taskContext.getDriverComparator(0);
		TypeComparator<IT2> comparator2 = this.taskContext.getDriverComparator(1);
		MutableObjectIterator<IT1> input1 = new CountingMutableObjectIterator<>(this.taskContext.<IT1>getInput(0), numRecordsIn);
		MutableObjectIterator<IT2> input2 = new CountingMutableObjectIterator<>(this.taskContext.<IT2>getInput(1), numRecordsIn);

		TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = 
				this.taskContext.getTaskConfig().getPairComparatorFactory(this.taskContext.getUserCodeClassLoader());

		double availableMemory = config.getRelativeMemoryDriver();
		boolean hashJoinUseBitMaps = taskContext.getTaskManagerInfo().getConfiguration()
			.getBoolean(AlgorithmOptions.HASH_JOIN_BLOOM_FILTERS);
		
		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (objectReuseEnabled) {
			if (buildSideIndex == 0 && probeSideIndex == 1) {

				matchIterator = new ReusingBuildFirstReOpenableHashJoinIterator<IT1, IT2, OT>(
						input1, input2,
						serializer1, comparator1,
						serializer2, comparator2,
						pairComparatorFactory.createComparator21(comparator1, comparator2),
						this.taskContext.getMemoryManager(),
						this.taskContext.getIOManager(),
						this.taskContext.getContainingTask(),
						availableMemory,
						false,
						false,
						hashJoinUseBitMaps);


			} else if (buildSideIndex == 1 && probeSideIndex == 0) {

				matchIterator = new ReusingBuildSecondReOpenableHashJoinIterator<IT1, IT2, OT>(
						input1, input2,
						serializer1, comparator1,
						serializer2, comparator2,
						pairComparatorFactory.createComparator12(comparator1, comparator2),
						this.taskContext.getMemoryManager(),
						this.taskContext.getIOManager(),
						this.taskContext.getContainingTask(),
						availableMemory,
						false,
						false,
						hashJoinUseBitMaps);

			} else {
				throw new Exception("Error: Inconsistent setup for repeatable hash join driver.");
			}
		} else {
			if (buildSideIndex == 0 && probeSideIndex == 1) {

				matchIterator = new NonReusingBuildFirstReOpenableHashJoinIterator<IT1, IT2, OT>(
						input1, input2,
						serializer1, comparator1,
						serializer2, comparator2,
						pairComparatorFactory.createComparator21(comparator1, comparator2),
						this.taskContext.getMemoryManager(),
						this.taskContext.getIOManager(),
						this.taskContext.getContainingTask(),
						availableMemory,
						false,
						false,
						hashJoinUseBitMaps);


			} else if (buildSideIndex == 1 && probeSideIndex == 0) {

				matchIterator = new NonReusingBuildSecondReOpenableHashJoinIterator<IT1, IT2, OT>(
						input1, input2,
						serializer1, comparator1,
						serializer2, comparator2,
						pairComparatorFactory.createComparator12(comparator1, comparator2),
						this.taskContext.getMemoryManager(),
						this.taskContext.getIOManager(),
						this.taskContext.getContainingTask(),
						availableMemory,
						false,
						false,
						hashJoinUseBitMaps);

			} else {
				throw new Exception("Error: Inconsistent setup for repeatable hash join driver.");
			}
		}
		
		this.matchIterator.open();
	}

	@Override
	public void prepare() throws Exception {
		// nothing
	}

	@Override
	public void run() throws Exception {
		final Counter numRecordsOut = taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
		final FlatJoinFunction<IT1, IT2, OT> matchStub = this.taskContext.getStub();
		final Collector<OT> collector = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
		
		while (this.running && matchIterator != null && matchIterator.callWithNextKey(matchStub, collector)) {
		}
	}

	@Override
	public void cleanup() throws Exception {}
	
	@Override
	public void reset() throws Exception {
		
		MutableObjectIterator<IT1> input1 = this.taskContext.getInput(0);
		MutableObjectIterator<IT2> input2 = this.taskContext.getInput(1);

		if (objectReuseEnabled) {
			if (buildSideIndex == 0 && probeSideIndex == 1) {
				final ReusingBuildFirstReOpenableHashJoinIterator<IT1, IT2, OT> matchIterator = (ReusingBuildFirstReOpenableHashJoinIterator<IT1, IT2, OT>) this.matchIterator;

				matchIterator.reopenProbe(input2);
			} else {
				final ReusingBuildSecondReOpenableHashJoinIterator<IT1, IT2, OT> matchIterator = (ReusingBuildSecondReOpenableHashJoinIterator<IT1, IT2, OT>) this.matchIterator;
				matchIterator.reopenProbe(input1);
			}
		} else {
			if (buildSideIndex == 0 && probeSideIndex == 1) {
				final NonReusingBuildFirstReOpenableHashJoinIterator<IT1, IT2, OT> matchIterator = (NonReusingBuildFirstReOpenableHashJoinIterator<IT1, IT2, OT>) this.matchIterator;

				matchIterator.reopenProbe(input2);
			} else {
				final NonReusingBuildSecondReOpenableHashJoinIterator<IT1, IT2, OT> matchIterator = (NonReusingBuildSecondReOpenableHashJoinIterator<IT1, IT2, OT>) this.matchIterator;
				matchIterator.reopenProbe(input1);
			}
		}
	}

	@Override
	public void teardown() {
		this.running = false;
		if (this.matchIterator != null) {
			this.matchIterator.close();
		}
	}

	@Override
	public void cancel() {
		this.running = false;
		if (this.matchIterator != null) {
			this.matchIterator.abort();
		}
	}
}
