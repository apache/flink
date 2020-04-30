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

import java.util.Collections;

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
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.SingleElementIterator;
import org.apache.flink.util.Collector;

public class CoGroupWithSolutionSetFirstDriver<IT1, IT2, OT> implements ResettableDriver<CoGroupFunction<IT1, IT2, OT>, OT> {
	
	private TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> taskContext;
	
	private CompactingHashTable<IT1> hashTable;
	
	private JoinHashMap<IT1> objectMap;
	
	private TypeSerializer<IT2> probeSideSerializer;
	
	private TypeComparator<IT2> probeSideComparator;

	private TypeSerializer<IT1> solutionSetSerializer;


	private TypePairComparator<IT2, IT1> pairComparator;
	
	private IT1 solutionSideRecord;
	
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
	public void initialize() {
		
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
		
		TypeComparatorFactory<IT2> probeSideComparatorFactory = config.getDriverComparator(0, classLoader);
		
		this.probeSideSerializer = taskContext.<IT2>getInputSerializer(0).getSerializer();
		this.probeSideComparator = probeSideComparatorFactory.createComparator();
		
		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (objectReuseEnabled) {
			solutionSideRecord = solutionSetSerializer.createInstance();
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

		final CoGroupFunction<IT1, IT2, OT> coGroupStub = taskContext.getStub();
		final Collector<OT> collector = taskContext.getOutputCollector();
		
		final SingleElementIterator<IT1> siIter = new SingleElementIterator<IT1>();
		final Iterable<IT1> emptySolutionSide = Collections.emptySet();

		if (objectReuseEnabled) {
			final ReusingKeyGroupedIterator<IT2> probeSideInput = new ReusingKeyGroupedIterator<IT2>(taskContext.<IT2>getInput(0), probeSideSerializer, probeSideComparator);
			if (this.hashTable != null) {
				final CompactingHashTable<IT1> join = hashTable;
				final CompactingHashTable<IT1>.HashTableProber<IT2> prober = join.getProber(this.probeSideComparator, this.pairComparator);


				IT1 buildSideRecord = solutionSideRecord;

				while (this.running && probeSideInput.nextKey()) {
					IT2 current = probeSideInput.getCurrent();

					IT1 matchedRecord = prober.getMatchFor(current, buildSideRecord);
					if (matchedRecord != null) {
						siIter.set(matchedRecord);
						coGroupStub.coGroup(siIter, probeSideInput.getValues(), collector);
					} else {
						coGroupStub.coGroup(emptySolutionSide, probeSideInput.getValues(), collector);
					}
				}
			} else {
				final JoinHashMap<IT1> join = this.objectMap;
				final JoinHashMap<IT1>.Prober<IT2> prober = join.createProber(this.probeSideComparator, this.pairComparator);
				final TypeSerializer<IT1> serializer = join.getBuildSerializer();

				while (this.running && probeSideInput.nextKey()) {
					IT2 current = probeSideInput.getCurrent();

					IT1 buildSideRecord = prober.lookupMatch(current);
					if (buildSideRecord != null) {
						siIter.set(serializer.copy(buildSideRecord));
						coGroupStub.coGroup(siIter, probeSideInput.getValues(), collector);
					} else {
						coGroupStub.coGroup(emptySolutionSide, probeSideInput.getValues(), collector);
					}
				}
			}
		} else {
			final NonReusingKeyGroupedIterator<IT2> probeSideInput = new NonReusingKeyGroupedIterator<IT2>(taskContext.<IT2>getInput(0), probeSideComparator);
			if (this.hashTable != null) {
				final CompactingHashTable<IT1> join = hashTable;
				final CompactingHashTable<IT1>.HashTableProber<IT2> prober = join.getProber(this
						.probeSideComparator, this.pairComparator);

				IT1 buildSideRecord;

				while (this.running && probeSideInput.nextKey()) {
					IT2 current = probeSideInput.getCurrent();

					buildSideRecord = prober.getMatchFor(current);
					if (buildSideRecord != null) {
						siIter.set(solutionSetSerializer.copy(buildSideRecord));
						coGroupStub.coGroup(siIter, probeSideInput.getValues(), collector);
					} else {
						coGroupStub.coGroup(emptySolutionSide, probeSideInput.getValues(), collector);
					}
				}
			} else {
				final JoinHashMap<IT1> join = this.objectMap;
				final JoinHashMap<IT1>.Prober<IT2> prober = join.createProber(this.probeSideComparator, this.pairComparator);
				final TypeSerializer<IT1> serializer = join.getBuildSerializer();

				while (this.running && probeSideInput.nextKey()) {
					IT2 current = probeSideInput.getCurrent();

					IT1 buildSideRecord = prober.lookupMatch(current);
					if (buildSideRecord != null) {
						siIter.set(serializer.copy(buildSideRecord));
						coGroupStub.coGroup(siIter, probeSideInput.getValues(), collector);
					} else {
						coGroupStub.coGroup(emptySolutionSide, probeSideInput.getValues(), collector);
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
