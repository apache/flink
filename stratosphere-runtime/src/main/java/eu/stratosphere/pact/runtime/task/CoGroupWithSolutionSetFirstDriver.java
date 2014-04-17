/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.util.Iterator;

import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.pact.runtime.hash.CompactingHashTable;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionSetBroker;
import eu.stratosphere.pact.runtime.iterative.task.AbstractIterativePactTask;
import eu.stratosphere.pact.runtime.util.EmptyIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.util.SingleElementIterator;
import eu.stratosphere.util.Collector;

public class CoGroupWithSolutionSetFirstDriver<IT1, IT2, OT> implements ResettablePactDriver<GenericCoGrouper<IT1, IT2, OT>, OT> {
	
	private PactTaskContext<GenericCoGrouper<IT1, IT2, OT>, OT> taskContext;
	
	private CompactingHashTable<IT1> hashTable;
	
	private TypeSerializer<IT2> probeSideSerializer;
	
	private TypeComparator<IT2> probeSideComparator;
	
	private TypePairComparator<IT2, IT1> pairComparator;
	
	private IT1 solutionSideRecord;
	
	protected volatile boolean running;

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void setup(PactTaskContext<GenericCoGrouper<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}
	
	@Override
	public Class<GenericCoGrouper<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericCoGrouper<IT1, IT2, OT>> clazz = (Class<GenericCoGrouper<IT1, IT2, OT>>) (Class<?>) GenericCoGrouper.class;
		return clazz;
	}
	
	@Override
	public boolean requiresComparatorOnInput() {
		return true;
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
			this.hashTable = (CompactingHashTable<IT1>) SolutionSetBroker.instance().get(identifier);
		} else {
			throw new RuntimeException("The task context of this driver is no iterative task context.");
		}
		
		TypeSerializer<IT1> buildSideSerializer = hashTable.getBuildSideSerializer();
		TypeComparator<IT1> buildSideComparator = hashTable.getBuildSideComparator().duplicate();
		
		probeSideSerializer = taskContext.getInputSerializer(0);
		probeSideComparator = taskContext.getInputComparator(0);
		
		solutionSideRecord = buildSideSerializer.createInstance();
		
		TypePairComparatorFactory<IT1, IT2> pairCompFactory = taskContext.getTaskConfig().getPairComparatorFactory(taskContext.getUserCodeClassLoader());
		pairComparator = pairCompFactory.createComparator21(buildSideComparator, probeSideComparator);
	}

	@Override
	public void prepare() {
		// nothing to prepare in each iteration
		// later, if we support out-of-core operation, we need to put the code in here
		// that brings the initial in-memory partitions into memory
	}

	@Override
	public void run() throws Exception {

		final GenericCoGrouper<IT1, IT2, OT> coGroupStub = taskContext.getStub();
		final Collector<OT> collector = taskContext.getOutputCollector();
		
		IT1 buildSideRecord = solutionSideRecord;
			
		final CompactingHashTable<IT1> join = hashTable;
		
		final KeyGroupedIterator<IT2> probeSideInput = new KeyGroupedIterator<IT2>(taskContext.<IT2>getInput(0), probeSideSerializer, probeSideComparator);
		final SingleElementIterator<IT1> siIter = new SingleElementIterator<IT1>();
		final Iterator<IT1> emptySolutionSide = EmptyIterator.<IT1>get();
		
		final CompactingHashTable<IT1>.HashTableProber<IT2> prober = join.getProber(this.probeSideComparator, this.pairComparator);
		
		while (this.running && probeSideInput.nextKey()) {
			IT2 current = probeSideInput.getCurrent();
			
			if (prober.getMatchFor(current, buildSideRecord)) {
				siIter.set(buildSideRecord);
				coGroupStub.coGroup(siIter, probeSideInput.getValues(), collector);
			}
			else {
				coGroupStub.coGroup(emptySolutionSide, probeSideInput.getValues(), collector);
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
