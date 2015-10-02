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
import org.apache.flink.api.common.functions.CoGroupFunction;
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
import org.apache.flink.runtime.util.EmptyIterator;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.SingleElementIterator;
import org.apache.flink.util.Collector;

public class CoGroupWithSolutionSetSecondDriver<IT1, IT2, OT> implements ResettableDriver<CoGroupFunction<IT1, IT2, OT>, OT> {
	
	private TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> taskContext;
	
	private CompactingHashTable<IT2> hashTable;
	
	private JoinHashMap<IT2> objectMap;
	
	private TypeSerializer<IT1> probeSideSerializer;
	
	private TypeComparator<IT1> probeSideComparator;

	private TypeSerializer<IT2> solutionSetSerializer;

	private TypePairComparator<IT1, IT2> pairComparator;
	
	private IT2 solutionSideRecord;
	
	protected volatile boolean running;

	private boolean objectReuseEnabled = false;

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void setup(TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> context) {
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

	@Override
	@SuppressWarnings("unchecked")
	public void initialize() throws Exception {
		
		final TypeComparator<IT2> solutionSetComparator;
		
		// grab a handle to the hash table from the iteration broker
		if (taskContext instanceof AbstractIterativeTask) {
			AbstractIterativeTask<?, ?> iterativeTaskContext = (AbstractIterativeTask<?, ?>) taskContext;
			String identifier = iterativeTaskContext.brokerKey();
			Object table = SolutionSetBroker.instance().get(identifier);
			
			if (table instanceof CompactingHashTable) {
				this.hashTable = (CompactingHashTable<IT2>) table;
				solutionSetSerializer = this.hashTable.getBuildSideSerializer();
				solutionSetComparator = this.hashTable.getBuildSideComparator().duplicate();
			}
			else if (table instanceof JoinHashMap) {
				this.objectMap = (JoinHashMap<IT2>) table;
				solutionSetSerializer = this.objectMap.getBuildSerializer();
				solutionSetComparator = this.objectMap.getBuildComparator().duplicate();
			}
			else {
				throw new RuntimeException("Unrecognized solution set index: " + table);
			}
		}
		else {
			throw new Exception("The task context of this driver is no iterative task context.");
		}
		
		TaskConfig config = taskContext.getTaskConfig();
		ClassLoader classLoader = taskContext.getUserCodeClassLoader();
		
		TypeComparatorFactory<IT1> probeSideComparatorFactory = config.getDriverComparator(0, classLoader); 
		
		this.probeSideSerializer = taskContext.<IT1>getInputSerializer(0).getSerializer();
		this.probeSideComparator = probeSideComparatorFactory.createComparator();

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (objectReuseEnabled) {
			solutionSideRecord = solutionSetSerializer.createInstance();
		};
		
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

		final CoGroupFunction<IT1, IT2, OT> coGroupStub = taskContext.getStub();
		final Collector<OT> collector = taskContext.getOutputCollector();

		final SingleElementIterator<IT2> siIter = new SingleElementIterator<IT2>();
		final Iterable<IT2> emptySolutionSide = EmptyIterator.<IT2>get();

		if (objectReuseEnabled) {
			final ReusingKeyGroupedIterator<IT1> probeSideInput = new ReusingKeyGroupedIterator<IT1>(taskContext.<IT1>getInput(0), probeSideSerializer, probeSideComparator);

			if (this.hashTable != null) {
				final CompactingHashTable<IT2> join = hashTable;
				final CompactingHashTable<IT2>.HashTableProber<IT1> prober = join.getProber(this.probeSideComparator, this.pairComparator);

				IT2 buildSideRecord = solutionSideRecord;

				while (this.running && probeSideInput.nextKey()) {
					IT1 current = probeSideInput.getCurrent();

					IT2 matchedRecord = prober.getMatchFor(current, buildSideRecord);
					if (matchedRecord != null) {
						siIter.set(matchedRecord);
						coGroupStub.coGroup(probeSideInput.getValues(), siIter, collector);
					} else {
						coGroupStub.coGroup(probeSideInput.getValues(), emptySolutionSide, collector);
					}
				}
			} else {
				final JoinHashMap<IT2> join = this.objectMap;
				final JoinHashMap<IT2>.Prober<IT1> prober = join.createProber(this.probeSideComparator, this.pairComparator);
				final TypeSerializer<IT2> serializer = join.getBuildSerializer();

				while (this.running && probeSideInput.nextKey()) {
					IT1 current = probeSideInput.getCurrent();

					IT2 buildSideRecord = prober.lookupMatch(current);
					if (buildSideRecord != null) {
						siIter.set(serializer.copy(buildSideRecord));
						coGroupStub.coGroup(probeSideInput.getValues(), siIter, collector);
					} else {
						coGroupStub.coGroup(probeSideInput.getValues(), emptySolutionSide, collector);
					}
				}
			}
		} else {
			final NonReusingKeyGroupedIterator<IT1> probeSideInput = 
					new NonReusingKeyGroupedIterator<IT1>(taskContext.<IT1>getInput(0), probeSideComparator);

			if (this.hashTable != null) {
				final CompactingHashTable<IT2> join = hashTable;
				final CompactingHashTable<IT2>.HashTableProber<IT1> prober = join.getProber(this.probeSideComparator, this.pairComparator);

				IT2 buildSideRecord;

				while (this.running && probeSideInput.nextKey()) {
					IT1 current = probeSideInput.getCurrent();

					buildSideRecord = prober.getMatchFor(current);
					if (buildSideRecord != null) {
						siIter.set(solutionSetSerializer.copy(buildSideRecord));
						coGroupStub.coGroup(probeSideInput.getValues(), siIter, collector);
					} else {
						coGroupStub.coGroup(probeSideInput.getValues(), emptySolutionSide, collector);
					}
				}
			} else {
				final JoinHashMap<IT2> join = this.objectMap;
				final JoinHashMap<IT2>.Prober<IT1> prober = join.createProber(this.probeSideComparator, this.pairComparator);
				final TypeSerializer<IT2> serializer = join.getBuildSerializer();

				while (this.running && probeSideInput.nextKey()) {
					IT1 current = probeSideInput.getCurrent();

					IT2 buildSideRecord = prober.lookupMatch(current);
					if (buildSideRecord != null) {
						siIter.set(serializer.copy(buildSideRecord));
						coGroupStub.coGroup(probeSideInput.getValues(), siIter, collector);
					} else {
						coGroupStub.coGroup(probeSideInput.getValues(), emptySolutionSide, collector);
					}
				}
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
