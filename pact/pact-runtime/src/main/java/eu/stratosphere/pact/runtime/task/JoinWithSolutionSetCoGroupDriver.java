/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.generic.stub.GenericCoGrouper;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionsetBroker;
import eu.stratosphere.pact.runtime.iterative.task.AbstractIterativePactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

public abstract class JoinWithSolutionSetCoGroupDriver<IT1, IT2, OT> implements ResettablePactDriver<GenericCoGrouper<IT1, IT2, OT>, OT> {
	
	protected PactTaskContext<GenericCoGrouper<IT1, IT2, OT>, OT> taskContext;
	
	protected MutableHashTable<?, ?> hashTable;
	
	private TypeSerializer<IT1> serializer1;
	private TypeSerializer<IT2> serializer2;
	private TypeComparator<IT1> comparator1;
	private TypeComparator<IT2> comparator2;
	
	private IT1 rec1;
	private IT2 rec2;
	
	protected volatile boolean running;

	// --------------------------------------------------------------------------------------------
	
	protected abstract int getSolutionSetInputIndex();
	
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

	@Override
	public void initialize() throws Exception {
		TaskConfig config = taskContext.getTaskConfig();
		ClassLoader classLoader = taskContext.getUserCodeClassLoader();
		
		int ssIndex = getSolutionSetInputIndex();
		if (ssIndex == 0) {
			TypeSerializerFactory<IT1> sSerializerFact = config.getSolutionSetSerializer(classLoader);
//			TypeComparatorFactory<IT1> sComparatorFact = config.getSolutionSetComparator(classLoader);
			serializer1 = sSerializerFact.getSerializer();
//			comparator1 = sComparatorFact.createComparator();
			serializer2 = taskContext.getInputSerializer(0);
			comparator2 = taskContext.getInputComparator(0);
		} else if (ssIndex == 1) {
			TypeSerializerFactory<IT2> sSerializerFact = config.getSolutionSetSerializer(classLoader);
//			TypeComparatorFactory<IT2> sComparatorFact = config.getSolutionSetComparator(classLoader);
			serializer1 = taskContext.getInputSerializer(0);
			comparator1 = taskContext.getInputComparator(0);
			serializer2 = sSerializerFact.getSerializer();
//			comparator2 = sComparatorFact.createComparator();
		} else {
			throw new Exception();
		}
		
		rec1 = serializer1.createInstance();
		rec2 = serializer2.createInstance();
		
		// grab a handle to the hash table from the iteration broker
		if (taskContext instanceof AbstractIterativePactTask) {
			AbstractIterativePactTask<?, ?> iterativeTaskContext = (AbstractIterativePactTask<?, ?>) taskContext;
			String identifyer = iterativeTaskContext.brokerKey();
			this.hashTable = SolutionsetBroker.instance().get(identifyer);
		} else {
			throw new Exception("The task context of this driver is no iterative task context.");
		}
	}

	@Override
	public void prepare() throws Exception {
		// nothing to prepare in each iteration
		// later, if we support out-of-core operation, we need to put the code in here
		// that brings the initial in-memory partitions into memory
	}

	@Override
	public void run() throws Exception {

		final GenericCoGrouper<IT1, IT2, OT> coGroupStub = taskContext.getStub();
		final Collector<OT> collector = taskContext.getOutputCollector();
		
		if (getSolutionSetInputIndex() == 0) {
			final IT1 buildSideRecord = rec1;
			
			@SuppressWarnings("unchecked")
			final MutableHashTable<IT1, IT2> join = (MutableHashTable<IT1, IT2>) hashTable;
			
			final KeyGroupedIterator<IT2> probeSideInput = new KeyGroupedIterator<IT2>(taskContext.<IT2>getInput(0), serializer2, comparator2);
			final SingleRecordIterator<IT1> siIter = new SingleRecordIterator<IT1>();
			
			while (this.running && probeSideInput.nextKey()) {
				IT2 current = probeSideInput.getCurrent();
				final MutableHashTable.HashBucketIterator<IT1, IT2> bucket = join.getMatchesFor(current);
				if (bucket.next(buildSideRecord)) {
					siIter.set(buildSideRecord);
					coGroupStub.coGroup(siIter, probeSideInput.getValues(), collector);
				}
				else {
					// no match found, this is for now an error case
					throw new RuntimeException("No Match found in solution set.");
				}
			}
		} else if (getSolutionSetInputIndex() == 1) {
			final IT2 buildSideRecord = rec2;
			
			@SuppressWarnings("unchecked")
			final MutableHashTable<IT2, IT1> join = (MutableHashTable<IT2, IT1>) hashTable;
			
			final KeyGroupedIterator<IT1> probeSideInput = new KeyGroupedIterator<IT1>(taskContext.<IT1>getInput(0), serializer1, comparator1);
			final SingleRecordIterator<IT2> siIter = new SingleRecordIterator<IT2>();
			
			while (this.running && probeSideInput.nextKey()) {
				IT1 current = probeSideInput.getCurrent();
				final MutableHashTable.HashBucketIterator<IT2, IT1> bucket = join.getMatchesFor(current);
				if (bucket.next(buildSideRecord)) {
					siIter.set(buildSideRecord);
					coGroupStub.coGroup(probeSideInput.getValues(), siIter, collector);
				}
				else {
					// no match found, this is for now an error case
					throw new RuntimeException("No Match found in solution set.");
				}
			}
		} else {
			throw new Exception();
		}
	}

	@Override
	public void cleanup() throws Exception {}
	
	@Override
	public void reset() throws Exception {}

	@Override
	public void teardown() {
		// hash table is torn down by the iteration head task
	}

	@Override
	public void cancel() {
		this.running = false;
	}
	
	private static final class SingleRecordIterator<E> implements Iterator<E> {
		
		private E current;
		private boolean available = false;
		
		void set(E current) {
			this.current = current;
			this.available = true;
		}

		@Override
		public boolean hasNext() {
			return available;
		}

		@Override
		public E next() {
			if (available) {
				available = false;
				return current;
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	public static final class SolutionSetFirstCoGroupDriver<IT1, IT2, OT> extends JoinWithSolutionSetCoGroupDriver<IT1, IT2, OT> {

		@Override
		protected int getSolutionSetInputIndex() {
			return 0;
		}
	}
	
	public static final class SolutionSetSecondCoGroupDriver<IT1, IT2, OT> extends JoinWithSolutionSetCoGroupDriver<IT1, IT2, OT> {

		@Override
		protected int getSolutionSetInputIndex() {
			return 1;
		}
	}
}
