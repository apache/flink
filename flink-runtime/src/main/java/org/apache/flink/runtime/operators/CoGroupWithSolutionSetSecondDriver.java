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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetBroker;
import org.apache.flink.runtime.iterative.task.AbstractIterativePactTask;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.runtime.util.EmptyIterator;
import org.apache.flink.runtime.util.KeyGroupedIterator;
import org.apache.flink.runtime.util.SingleElementIterator;
import org.apache.flink.util.Collector;

public class CoGroupWithSolutionSetSecondDriver<IT1, IT2, OT> implements ResettablePactDriver<CoGroupFunction<IT1, IT2, OT>, OT> {
	
	private PactTaskContext<CoGroupFunction<IT1, IT2, OT>, OT> taskContext;
	
	private CompactingHashTable<IT2> hashTable;
	
	private TypeSerializer<IT1> probeSideSerializer;
	
	private TypeComparator<IT1> probeSideComparator;
	
	private TypePairComparator<IT1, IT2> pairComparator;
	
	private IT2 solutionSideRecord;
	
	protected volatile boolean running;

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void setup(PactTaskContext<CoGroupFunction<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}
	
	@Override
	public Class<CoGroupFunction<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<CoGroupFunction<IT1, IT2, OT>> clazz = (Class<CoGroupFunction<IT1, IT2, OT>>) (Class<?>) CoGroupFunction.class;
		return clazz;
	}
	
	@Override
	public int getNumberOfDriverComparators() {
		return 1;
	}
	
	@Override
	public boolean isInputResettable(int inputNum) {
		if (inputNum < 0 || inputNum > 1) {
			throw new IndexOutOfBoundsException();
		}
		
		// from the perspective of the task that runs this operator, there is only one input, which is not resettable
		// we implement the resettable interface only in order to avoid that this class is re-instantiated for
		// every iteration
		return false;
	}
	
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public void initialize() {
		// grab a handle to the hash table from the iteration broker
		if (taskContext instanceof AbstractIterativePactTask) {
			AbstractIterativePactTask<?, ?> iterativeTaskContext = (AbstractIterativePactTask<?, ?>) taskContext;
			String identifier = iterativeTaskContext.brokerKey();
			this.hashTable = (CompactingHashTable<IT2>) SolutionSetBroker.instance().get(identifier);
		} else {
			throw new RuntimeException("The task context of this driver is no iterative task context.");
		}
		
		TypeSerializer<IT2> buildSideSerializer = hashTable.getBuildSideSerializer();
		TypeComparator<IT2> buildSideComparator = hashTable.getBuildSideComparator().duplicate();
		
		probeSideSerializer = taskContext.<IT1>getInputSerializer(0).getSerializer();
		probeSideComparator = taskContext.getDriverComparator(0);
		
		solutionSideRecord = buildSideSerializer.createInstance();
		
		TypePairComparatorFactory<IT1, IT2> pairCompFactory = taskContext.getTaskConfig().getPairComparatorFactory(taskContext.getUserCodeClassLoader());
		pairComparator = pairCompFactory.createComparator12(probeSideComparator, buildSideComparator);
	}

	@Override
	public void prepare() {
		// nothing to prepare in each iteration
		// later, if we support out-of-core operation, we need to put the code in here
		// that brings the initial in-memory partitions into memory
	}

	@Override
	public void run() throws Exception {

		final CoGroupFunction<IT1, IT2, OT> coGroupStub = taskContext.getStub();
		final Collector<OT> collector = taskContext.getOutputCollector();
		
		IT2 buildSideRecord = solutionSideRecord;
			
		final CompactingHashTable<IT2> join = hashTable;
		
		final KeyGroupedIterator<IT1> probeSideInput = new KeyGroupedIterator<IT1>(taskContext.<IT1>getInput(0), probeSideSerializer, probeSideComparator);
		final SingleElementIterator<IT2> siIter = new SingleElementIterator<IT2>();
		final Iterable<IT2> emptySolutionSide = EmptyIterator.<IT2>get();
		
		final CompactingHashTable<IT2>.HashTableProber<IT1> prober = join.getProber(this.probeSideComparator, this.pairComparator);
		
		while (this.running && probeSideInput.nextKey()) {
			IT1 current = probeSideInput.getCurrent();

			buildSideRecord = prober.getMatchFor(current, buildSideRecord);
			if (buildSideRecord != null) {
				siIter.set(buildSideRecord);
				coGroupStub.coGroup(probeSideInput.getValues(), siIter, collector);
			}
			else {
				coGroupStub.coGroup(probeSideInput.getValues(), emptySolutionSide, collector);
			}
		}
	}

	@Override
	public void cleanup() {}
	
	@Override
	public void reset() {}

	@Override
	public void teardown() {
		// hash table is torn down by the iteration head task
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}
