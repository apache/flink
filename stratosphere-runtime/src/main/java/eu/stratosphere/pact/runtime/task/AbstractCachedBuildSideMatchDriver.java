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

import java.util.List;

import eu.stratosphere.api.functions.GenericMatcher;
import eu.stratosphere.api.typeutils.TypeComparator;
import eu.stratosphere.api.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

public abstract class AbstractCachedBuildSideMatchDriver<IT1, IT2, OT> extends MatchDriver<IT1, IT2, OT> implements ResettablePactDriver<GenericMatcher<IT1, IT2, OT>, OT> {
	
	/**
	 * We keep it without generic parameters, because they vary depending on which input is the build side.
	 */
	protected volatile MutableHashTable<?, ?> hashJoin;

	
	protected abstract int getBuildSideIndex();
	
	protected abstract int getProbeSideIndex();
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean isInputResettable(int inputNum) {
		if (inputNum < 0 || inputNum > 1) {
			throw new IndexOutOfBoundsException();
		}
		return inputNum == getBuildSideIndex();
	}

	@Override
	public void initialize() throws Exception {
		TaskConfig config = this.taskContext.getTaskConfig();
		
		TypeSerializer<IT1> serializer1 = this.taskContext.getInputSerializer(0);
		TypeSerializer<IT2> serializer2 = this.taskContext.getInputSerializer(1);
		TypeComparator<IT1> comparator1 = this.taskContext.getInputComparator(0);
		TypeComparator<IT2> comparator2 = this.taskContext.getInputComparator(1);
		MutableObjectIterator<IT1> input1 = this.taskContext.getInput(0);
		MutableObjectIterator<IT2> input2 = this.taskContext.getInput(1);

		TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = 
				this.taskContext.getTaskConfig().getPairComparatorFactory(this.taskContext.getUserCodeClassLoader());

		List<MemorySegment> memSegments = this.taskContext.getMemoryManager().allocatePages(
			this.taskContext.getOwningNepheleTask(), config.getMemoryDriver());

		if (getBuildSideIndex() == 0 && getProbeSideIndex() == 1) {
			MutableHashTable<IT1, IT2> hashJoin = new MutableHashTable<IT1, IT2>(serializer1, serializer2, comparator1, comparator2,
					pairComparatorFactory.createComparator21(comparator1, comparator2), memSegments, this.taskContext.getIOManager());
			this.hashJoin = hashJoin;
			hashJoin.open(input1, EmptyMutableObjectIterator.<IT2>get());
		} else if (getBuildSideIndex() == 1 && getProbeSideIndex() == 0) {
			MutableHashTable<IT2, IT1> hashJoin = new MutableHashTable<IT2, IT1>(serializer2, serializer1, comparator2, comparator1,
					pairComparatorFactory.createComparator12(comparator1, comparator2), memSegments, this.taskContext.getIOManager());
			this.hashJoin = hashJoin;
			hashJoin.open(input2, EmptyMutableObjectIterator.<IT1>get());
		} else {
			throw new Exception("Error: Inconcistent setup for repeatable hash join driver.");
		}
	}

	@Override
	public void prepare() throws Exception {
		// nothing
	}

	@Override
	public void run() throws Exception {

		final GenericMatcher<IT1, IT2, OT> matchStub = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		
		if (getBuildSideIndex() == 0) {
			final TypeSerializer<IT1> buildSideSerializer = taskContext.<IT1> getInputSerializer(0);
			final TypeSerializer<IT2> probeSideSerializer = taskContext.<IT2> getInputSerializer(1);
			
			final IT1 buildSideRecordFirst = buildSideSerializer.createInstance();
			final IT1 buildSideRecordOther = buildSideSerializer.createInstance();
			final IT2 probeSideRecord = probeSideSerializer.createInstance();
			final IT2 probeSideRecordCopy = probeSideSerializer.createInstance();
			
			@SuppressWarnings("unchecked")
			final MutableHashTable<IT1, IT2> join = (MutableHashTable<IT1, IT2>) this.hashJoin;
			
			final MutableObjectIterator<IT2> probeSideInput = taskContext.<IT2>getInput(1);
			
			while (this.running && probeSideInput.next(probeSideRecord)) {
				final MutableHashTable.HashBucketIterator<IT1, IT2> bucket = join.getMatchesFor(probeSideRecord);
				
				if (bucket.next(buildSideRecordFirst)) {
					while (bucket.next(buildSideRecordOther)) {
						probeSideSerializer.copyTo(probeSideRecord, probeSideRecordCopy);
						matchStub.match(buildSideRecordOther, probeSideRecordCopy, collector);
					}
					matchStub.match(buildSideRecordFirst, probeSideRecord, collector);
				}
			}
		} else if (getBuildSideIndex() == 1) {
			final TypeSerializer<IT2> buildSideSerializer = taskContext.<IT2> getInputSerializer(1);
			final TypeSerializer<IT1> probeSideSerializer = taskContext.<IT1> getInputSerializer(0);
			
			final IT2 buildSideRecordFirst = buildSideSerializer.createInstance();
			final IT2 buildSideRecordOther = buildSideSerializer.createInstance();
			final IT1 probeSideRecord = probeSideSerializer.createInstance();
			final IT1 probeSideRecordCopy = probeSideSerializer.createInstance();
			
			@SuppressWarnings("unchecked")
			final MutableHashTable<IT2, IT1> join = (MutableHashTable<IT2, IT1>) this.hashJoin;
			
			final MutableObjectIterator<IT1> probeSideInput = taskContext.<IT1>getInput(0);
			
			while (this.running && probeSideInput.next(probeSideRecord)) {
				final MutableHashTable.HashBucketIterator<IT2, IT1> bucket = join.getMatchesFor(probeSideRecord);
				
				if (bucket.next(buildSideRecordFirst)) {
					while (bucket.next(buildSideRecordOther)) {
						probeSideSerializer.copyTo(probeSideRecord, probeSideRecordCopy);
						matchStub.match(probeSideRecordCopy, buildSideRecordOther, collector);
					}
					matchStub.match(probeSideRecord, buildSideRecordFirst, collector);
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
		this.hashJoin.close();
	}

	@Override
	public void cancel() {
		this.running = false;
		if (this.hashJoin != null) {
			this.hashJoin.close();
		}
	}
}
