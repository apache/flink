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
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetBroker;
import org.apache.flink.runtime.iterative.task.AbstractIterativePactTask;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class JoinWithSolutionSetSecondDriver<IT1, IT2, OT> implements ResettablePactDriver<FlatJoinFunction<IT1, IT2, OT>, OT> {
	
	private PactTaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> taskContext;
	
	private CompactingHashTable<IT2> hashTable;
	
	private TypeComparator<IT1> probeSideComparator;
	
	private TypePairComparator<IT1, IT2> pairComparator;
	
	private IT2 solutionSideRecord;
	private IT1 probeSideRecord;
	
	protected volatile boolean running;
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void setup(PactTaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}
	
	@Override
	public Class<FlatJoinFunction<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<FlatJoinFunction<IT1, IT2, OT>> clazz = (Class<FlatJoinFunction<IT1, IT2, OT>>) (Class<?>) FlatJoinFunction.class;
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
		// every iterations
		return false;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	@SuppressWarnings("unchecked")
	public void initialize() throws Exception {
		
		// grab a handle to the hash table from the iteration broker
		if (taskContext instanceof AbstractIterativePactTask) {
			AbstractIterativePactTask<?, ?> iterativeTaskContext = (AbstractIterativePactTask<?, ?>) taskContext;
			String identifier = iterativeTaskContext.brokerKey();
			this.hashTable = (CompactingHashTable<IT2>) SolutionSetBroker.instance().get(identifier);
		} else {
			throw new Exception("The task context of this driver is no iterative task context.");
		}
		
		TaskConfig config = taskContext.getTaskConfig();
		ClassLoader classLoader = taskContext.getUserCodeClassLoader();
		
		TypeSerializer<IT2> solutionSetSerializer = this.hashTable.getBuildSideSerializer();
		TypeSerializer<IT1> probeSideSerializer = taskContext.<IT1>getInputSerializer(0).getSerializer();
		
		TypeComparatorFactory<IT1> probeSideComparatorFactory = config.getDriverComparator(0, classLoader);
		TypeComparator<IT2> solutionSetComparator = this.hashTable.getBuildSideComparator().duplicate();
		this.probeSideComparator = probeSideComparatorFactory.createComparator();
		
		solutionSideRecord = solutionSetSerializer.createInstance();
		probeSideRecord = probeSideSerializer.createInstance();
		
		TypePairComparatorFactory<IT1, IT2> factory = taskContext.getTaskConfig().getPairComparatorFactory(taskContext.getUserCodeClassLoader());
		pairComparator = factory.createComparator12(this.probeSideComparator, solutionSetComparator);
	}

	@Override
	public void prepare() {
		// nothing to prepare in each iteration
		// later, if we support out-of-core operation, we need to put the code in here
		// that brings the initial in-memory partitions into memory
	}

	@Override
	public void run() throws Exception {

		final FlatJoinFunction<IT1, IT2, OT> joinFunction = taskContext.getStub();
		final Collector<OT> collector = taskContext.getOutputCollector();
		
		IT2 buildSideRecord = this.solutionSideRecord;
		IT1 probeSideRecord = this.probeSideRecord;
			
		final CompactingHashTable<IT2> join = hashTable;
		final MutableObjectIterator<IT1> probeSideInput = taskContext.getInput(0);
			
		final CompactingHashTable<IT2>.HashTableProber<IT1> prober = join.getProber(probeSideComparator, pairComparator);
		while (this.running && ((probeSideRecord = probeSideInput.next(probeSideRecord)) != null)) {
			buildSideRecord = prober.getMatchFor(probeSideRecord, buildSideRecord);
			joinFunction.join(probeSideRecord, buildSideRecord, collector);
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
