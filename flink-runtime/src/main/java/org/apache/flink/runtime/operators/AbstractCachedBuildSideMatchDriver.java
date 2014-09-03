/**
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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.hash.BuildFirstReOpenableHashMatchIterator;
import org.apache.flink.runtime.operators.hash.BuildSecondReOpenableHashMatchIterator;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public abstract class AbstractCachedBuildSideMatchDriver<IT1, IT2, OT> extends MatchDriver<IT1, IT2, OT> implements ResettablePactDriver<FlatJoinFunction<IT1, IT2, OT>, OT> {

	private volatile JoinTaskIterator<IT1, IT2, OT> matchIterator;
	
	private final int buildSideIndex;
	
	private final int probeSideIndex;
	
	protected AbstractCachedBuildSideMatchDriver(int buildSideIndex, int probeSideIndex) {
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
		
		TypeSerializer<IT1> serializer1 = this.taskContext.<IT1>getInputSerializer(0).getSerializer();
		TypeSerializer<IT2> serializer2 = this.taskContext.<IT2>getInputSerializer(1).getSerializer();
		TypeComparator<IT1> comparator1 = this.taskContext.getInputComparator(0);
		TypeComparator<IT2> comparator2 = this.taskContext.getInputComparator(1);
		MutableObjectIterator<IT1> input1 = this.taskContext.getInput(0);
		MutableObjectIterator<IT2> input2 = this.taskContext.getInput(1);

		TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = 
				this.taskContext.getTaskConfig().getPairComparatorFactory(this.taskContext.getUserCodeClassLoader());

		double availableMemory = config.getRelativeMemoryDriver();

		if (buildSideIndex == 0 && probeSideIndex == 1) {
			
			matchIterator = 
					new BuildFirstReOpenableHashMatchIterator<IT1, IT2, OT>(input1, input2, 
							serializer1, comparator1, 
							serializer2, comparator2, 
							pairComparatorFactory.createComparator21(comparator1, comparator2), 
							this.taskContext.getMemoryManager(),
							this.taskContext.getIOManager(),
							this.taskContext.getOwningNepheleTask(),
							availableMemory
							);
			
		} else if (buildSideIndex == 1 && probeSideIndex == 0) {

			matchIterator = 
					new BuildSecondReOpenableHashMatchIterator<IT1, IT2, OT>(input1, input2, 
							serializer1, comparator1, 
							serializer2, comparator2, 
							pairComparatorFactory.createComparator12(comparator1, comparator2), 
							this.taskContext.getMemoryManager(),
							this.taskContext.getIOManager(),
							this.taskContext.getOwningNepheleTask(),
							availableMemory
							);
			
		} else {
			throw new Exception("Error: Inconcistent setup for repeatable hash join driver.");
		}
		
		this.matchIterator.open();
	}

	@Override
	public void prepare() throws Exception {
		// nothing
	}

	@Override
	public void run() throws Exception {

		final FlatJoinFunction<IT1, IT2, OT> matchStub = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		
		if (buildSideIndex == 0) {
			
			final BuildFirstReOpenableHashMatchIterator<IT1, IT2, OT> matchIterator = (BuildFirstReOpenableHashMatchIterator<IT1, IT2, OT>) this.matchIterator;
			
			while (this.running && matchIterator != null && matchIterator.callWithNextKey(matchStub, collector));
			
		} else if (buildSideIndex == 1) {
			
			final BuildSecondReOpenableHashMatchIterator<IT1, IT2, OT> matchIterator = (BuildSecondReOpenableHashMatchIterator<IT1, IT2, OT>) this.matchIterator;
			
			while (this.running && matchIterator != null && matchIterator.callWithNextKey(matchStub, collector));
			
		} else {
			throw new Exception();
		}
	}

	@Override
	public void cleanup() throws Exception {}
	
	@Override
	public void reset() throws Exception {
		
		MutableObjectIterator<IT1> input1 = this.taskContext.getInput(0);
		MutableObjectIterator<IT2> input2 = this.taskContext.getInput(1);
		
		if (buildSideIndex == 0 && probeSideIndex == 1) {
			final BuildFirstReOpenableHashMatchIterator<IT1, IT2, OT> matchIterator = (BuildFirstReOpenableHashMatchIterator<IT1, IT2, OT>) this.matchIterator;
			matchIterator.reopenProbe(input2);
		}
		else {
			final BuildSecondReOpenableHashMatchIterator<IT1, IT2, OT> matchIterator = (BuildSecondReOpenableHashMatchIterator<IT1, IT2, OT>) this.matchIterator;
			matchIterator.reopenProbe(input1);
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
