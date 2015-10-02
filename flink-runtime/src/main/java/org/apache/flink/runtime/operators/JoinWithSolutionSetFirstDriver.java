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
import org.apache.flink.api.common.operators.util.JoinHashMap;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetBroker;
import org.apache.flink.runtime.iterative.task.AbstractIterativeTask;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class JoinWithSolutionSetFirstDriver<IT1, IT2, OT> implements ResettableDriver<FlatJoinFunction<IT1, IT2, OT>, OT> {
	
	private TaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> taskContext;
	
	private CompactingHashTable<IT1> hashTable;
	
	private JoinHashMap<IT1> objectMap;
	
	private TypeComparator<IT2> probeSideComparator;
	
	private TypePairComparator<IT2, IT1> pairComparator;
	
	private IT1 solutionSideRecord;
	private IT2 probeSideRecord;
	
	protected volatile boolean running;

	private boolean objectReuseEnabled = false;

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void setup(TaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> context) {
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
		// every iterations
		return false;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	@SuppressWarnings("unchecked")
	public void initialize() {
		
		final TypeSerializer<IT1> solutionSetSerializer;
		final TypeComparator<IT1> solutionSetComparator;
		
		// grab a handle to the hash table from the iteration broker
		if (taskContext instanceof AbstractIterativeTask) {
			AbstractIterativeTask<?, ?> iterativeTaskContext = (AbstractIterativeTask<?, ?>) taskContext;
			String identifier = iterativeTaskContext.brokerKey();
			
			Object table = SolutionSetBroker.instance().get(identifier);
			if (table instanceof CompactingHashTable) {
				this.hashTable = (CompactingHashTable<IT1>) table;
				solutionSetSerializer = this.hashTable.getBuildSideSerializer();
				solutionSetComparator = this.hashTable.getBuildSideComparator().duplicate();
			}
			else if (table instanceof JoinHashMap) {
				this.objectMap = (JoinHashMap<IT1>) table;
				solutionSetSerializer = this.objectMap.getBuildSerializer();
				solutionSetComparator = this.objectMap.getBuildComparator().duplicate();
			}
			else {
				throw new RuntimeException("Unrecognized solution set index: " + table);
			}
		} else {
			throw new RuntimeException("The task context of this driver is no iterative task context.");
		}
		
		TaskConfig config = taskContext.getTaskConfig();
		ClassLoader classLoader = taskContext.getUserCodeClassLoader();
		
		TypeSerializer<IT2> probeSideSerializer = taskContext.<IT2>getInputSerializer(0).getSerializer();
		
		TypeComparatorFactory<IT2> probeSideComparatorFactory = config.getDriverComparator(0, classLoader);
		this.probeSideComparator = probeSideComparatorFactory.createComparator();

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (objectReuseEnabled) {
			solutionSideRecord = solutionSetSerializer.createInstance();
			probeSideRecord = probeSideSerializer.createInstance();
		}

		TypePairComparatorFactory<IT1, IT2> factory = taskContext.getTaskConfig().getPairComparatorFactory(taskContext.getUserCodeClassLoader());
		pairComparator = factory.createComparator21(solutionSetComparator, this.probeSideComparator);
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
		final MutableObjectIterator<IT2> probeSideInput = taskContext.<IT2>getInput(0);
		

		if (objectReuseEnabled) {
			IT2 probeSideRecord = this.probeSideRecord;

			if (hashTable != null) {
				final CompactingHashTable<IT1> join = hashTable;
				final CompactingHashTable<IT1>.HashTableProber<IT2> prober = join.getProber(probeSideComparator, pairComparator);


				IT1 buildSideRecord = this.solutionSideRecord;

				while (this.running && ((probeSideRecord = probeSideInput.next(probeSideRecord)) != null)) {
					IT1 matchedRecord = prober.getMatchFor(probeSideRecord, buildSideRecord);
					joinFunction.join(matchedRecord, probeSideRecord, collector);
				}
			} else if (objectMap != null) {
				final JoinHashMap<IT1> hashTable = this.objectMap;
				final JoinHashMap<IT1>.Prober<IT2> prober = this.objectMap.createProber(probeSideComparator, pairComparator);
				final TypeSerializer<IT1> buildSerializer = hashTable.getBuildSerializer();

				while (this.running && ((probeSideRecord = probeSideInput.next(probeSideRecord)) != null)) {
					IT1 match = prober.lookupMatch(probeSideRecord);
					joinFunction.join(buildSerializer.copy(match), probeSideRecord, collector);
				}
			} else {
				throw new RuntimeException();
			}
		} else {
			IT2 probeSideRecord;

			if (hashTable != null) {
				final CompactingHashTable<IT1> join = hashTable;
				final CompactingHashTable<IT1>.HashTableProber<IT2> prober = join.getProber(probeSideComparator, pairComparator);


				IT1 buildSideRecord;

				while (this.running && ((probeSideRecord = probeSideInput.next()) != null)) {
					buildSideRecord = prober.getMatchFor(probeSideRecord);
					joinFunction.join(buildSideRecord, probeSideRecord, collector);
				}
			} else if (objectMap != null) {
				final JoinHashMap<IT1> hashTable = this.objectMap;
				final JoinHashMap<IT1>.Prober<IT2> prober = this.objectMap.createProber(probeSideComparator, pairComparator);
				final TypeSerializer<IT1> buildSerializer = hashTable.getBuildSerializer();

				while (this.running && ((probeSideRecord = probeSideInput.next()) != null)) {
					IT1 match = prober.lookupMatch(probeSideRecord);
					joinFunction.join(buildSerializer.copy(match), probeSideRecord, collector);
				}
			} else {
				throw new RuntimeException();
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
