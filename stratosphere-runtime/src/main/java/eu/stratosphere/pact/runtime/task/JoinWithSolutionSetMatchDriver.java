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

import eu.stratosphere.api.functions.GenericJoiner;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.api.typeutils.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionSetBroker;
import eu.stratosphere.pact.runtime.iterative.task.AbstractIterativePactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

public abstract class JoinWithSolutionSetMatchDriver<IT1, IT2, OT> implements ResettablePactDriver<GenericJoiner<IT1, IT2, OT>, OT> {
	
	protected PactTaskContext<GenericJoiner<IT1, IT2, OT>, OT> taskContext;
	
	protected MutableHashTable<?, ?> hashTable;
	
	private TypeSerializer<IT1> serializer1;
	private TypeSerializer<IT2> serializer2;
//	private TypeComparator<IT1> comparator1;
//	private TypeComparator<IT2> comparator2;
	
	private IT1 rec1;
	private IT2 rec2;
	
	protected volatile boolean running;

	// --------------------------------------------------------------------------------------------
	
	protected abstract int getSolutionSetInputIndex();
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void setup(PactTaskContext<GenericJoiner<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}
	
	@Override
	public Class<GenericJoiner<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericJoiner<IT1, IT2, OT>> clazz = (Class<GenericJoiner<IT1, IT2, OT>>) (Class<?>) GenericJoiner.class;
		return clazz;
	}
	
	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}
	
	@Override
	public boolean isInputResettable(int inputNum) {
		if (inputNum < 0 || inputNum > 1) {
			throw new IndexOutOfBoundsException();
		}
		
		// from the perspective of the task that runs this operator, there is only one input, which is not resettable
		// we implement the resettable interface only in order to avoid that this class is re-instantiated for
		// every iterations
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
//			comparator2 = taskContext.getInputComparator(0);
		} else if (ssIndex == 1) {
			TypeSerializerFactory<IT2> sSerializerFact = config.getSolutionSetSerializer(classLoader);
//			TypeComparatorFactory<IT2> sComparatorFact = config.getSolutionSetComparator(classLoader);
			serializer1 = taskContext.getInputSerializer(0);
//			comparator1 = taskContext.getInputComparator(0);
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
			this.hashTable = SolutionSetBroker.instance().get(identifyer);
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

		final GenericJoiner<IT1, IT2, OT> matchStub = taskContext.getStub();
		final Collector<OT> collector = taskContext.getOutputCollector();
		
		if (getSolutionSetInputIndex() == 0) {
			final IT1 buildSideRecord = rec1;
			final IT2 probeSideRecord = rec2;
			
			@SuppressWarnings("unchecked")
			final MutableHashTable<IT1, IT2> join = (MutableHashTable<IT1, IT2>) hashTable;
			final MutableObjectIterator<IT2> probeSideInput = taskContext.<IT2>getInput(0);
			
			while (this.running && probeSideInput.next(probeSideRecord)) {
				final MutableHashTable.HashBucketIterator<IT1, IT2> bucket = join.getMatchesFor(probeSideRecord);
				if (bucket.next(buildSideRecord)) {
					matchStub.match(buildSideRecord, probeSideRecord, collector);
				} else {
					// no match found, this is for now an error case
					throw new RuntimeException("No Match found in solution set.");
				}
			}
		} else if (getSolutionSetInputIndex() == 1) {
			final IT2 buildSideRecord = rec2;
			final IT1 probeSideRecord = rec1;
			
			@SuppressWarnings("unchecked")
			final MutableHashTable<IT2, IT1> join = (MutableHashTable<IT2, IT1>) hashTable;
			final MutableObjectIterator<IT1> probeSideInput = taskContext.<IT1>getInput(0);
			
			while (this.running && probeSideInput.next(probeSideRecord)) {
				final MutableHashTable.HashBucketIterator<IT2, IT1> bucket = join.getMatchesFor(probeSideRecord);
				if (bucket.next(buildSideRecord)) {
					matchStub.match(probeSideRecord, buildSideRecord, collector);
				} else {
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
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	public static final class SolutionSetFirstJoinDriver<IT1, IT2, OT> extends JoinWithSolutionSetMatchDriver<IT1, IT2, OT> {

		@Override
		protected int getSolutionSetInputIndex() {
			return 0;
		}
	}
	
	public static final class SolutionSetSecondJoinDriver<IT1, IT2, OT> extends JoinWithSolutionSetMatchDriver<IT1, IT2, OT> {

		@Override
		protected int getSolutionSetInputIndex() {
			return 1;
		}
	}
}
