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

package eu.stratosphere.pact.runtime.iterative.driver;


import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.generic.GenericMatcher;
import eu.stratosphere.pact.common.generic.types.TypeComparator;

import eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.task.MatchDriver;

import java.util.List;

//TODO make build and probeside indexes configurable
public abstract class AbstractRepeatableMatchDriver<IT1, IT2, OT> extends MatchDriver<IT1, IT2, OT> {

  protected volatile MutableHashTable<IT2, IT1> hashJoin;

  protected static final int PROBESIDE_INDEX = 0;
  protected static final int BUILDSIDE_INDEX = 1;

  private boolean firstIteration = true;

  @Override
  public void prepare() throws Exception {

    if (!firstIteration) {
      return;
    }

    TypeSerializer<IT1> probesideSerializer = taskContext.getInputSerializer(PROBESIDE_INDEX);
    TypeSerializer<IT2> buildsideSerializer = taskContext.getInputSerializer(BUILDSIDE_INDEX);
    TypeComparator<IT1> probesideComparator = taskContext.getInputComparator(PROBESIDE_INDEX);
    TypeComparator<IT2> buildSideComparator = taskContext.getInputComparator(BUILDSIDE_INDEX);

    final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = instantiateTypeComparatorFactory();

    List<MemorySegment> memSegments = taskContext.getMemoryManager().allocatePages(taskContext.getOwningNepheleTask(),
        getMemorySizeForHashjoin());

    hashJoin = new MutableHashTable<IT2, IT1>(buildsideSerializer, probesideSerializer, buildSideComparator,
        probesideComparator, pairComparatorFactory.createComparator12(probesideComparator, buildSideComparator),
        memSegments, taskContext.getIOManager());
  }

  protected boolean isFirstIteration() {
    return firstIteration;
  }

  protected abstract long getMemorySizeForHashjoin();

  protected abstract void beforeIteration(MutableObjectIterator<IT1> probeSide, MutableObjectIterator<IT2> buildSide)
      throws Exception;

  protected abstract MutableObjectIterator<IT1> getProbeSideForMatch();

  protected abstract void afterIteration();

  @Override
  public void run() throws Exception {

    final GenericMatcher<IT1, IT2, OT> matchStub = taskContext.getStub();
    final Collector<OT> collector = taskContext.getOutputCollector();
    final MutableObjectIterator<IT1> probeSide = taskContext.getInput(PROBESIDE_INDEX);
    final MutableObjectIterator<IT2> buildSide = taskContext.getInput(BUILDSIDE_INDEX);

    final MutableHashTable<IT2, IT1> hashJoin = Preconditions.checkNotNull(this.hashJoin);

    beforeIteration(probeSide, buildSide);

    final IT1 probeSideRecord = taskContext.<IT1>getInputSerializer(PROBESIDE_INDEX).createInstance();
    final IT2 buildSideRecord = taskContext.<IT2>getInputSerializer(BUILDSIDE_INDEX).createInstance();

    final MutableObjectIterator<IT1> probeSideForMatch = getProbeSideForMatch();

    while (running && probeSideForMatch.next(probeSideRecord)) {
      MutableHashTable.HashBucketIterator<IT2, IT1> bucket = hashJoin.getMatchesFor(probeSideRecord);
      while (bucket.next(buildSideRecord)) {
        matchStub.match(probeSideRecord, buildSideRecord, collector);
      }
    }
  }

  @Override
  public void cleanup() throws Exception {
    firstIteration = false;
    afterIteration();
  }

  public void finalCleanup() {
    hashJoin.close();
  }

  @Override
  public void cancel() {
    running = false;
    if (hashJoin != null) {
      hashJoin.close();
    }
  }

}
