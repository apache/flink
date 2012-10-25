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

package eu.stratosphere.pact.runtime.iterative.task;

import com.google.common.base.Preconditions;
import eu.stratosphere.pact.common.generic.GenericMatcher;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;
import eu.stratosphere.pact.runtime.iterative.io.UpdateSolutionsetOutputCollector;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SolutionsetMatchDriver<IT1, IT2, OT> implements PactDriver<GenericMatcher<IT1, IT2, OT>, OT> {

  protected PactTaskContext<GenericMatcher<IT1, IT2, OT>, OT> taskContext;

  private UpdateSolutionsetOutputCollector<OT> collector;

  private boolean firstIteration = true;

  //TODO typesafety
  private volatile MutableHashTable hashJoin;

  private volatile boolean running;

  private static final Log log = LogFactory.getLog(SolutionsetMatchDriver.class);

  void injectHashJoin(MutableHashTable hashJoin) {
    this.hashJoin = hashJoin;
  }

  @Override
  public void setup(PactTaskContext<GenericMatcher<IT1, IT2, OT>, OT> context) {
    taskContext = context;
    running = true;
  }

  @Override
  public int getNumberOfInputs() {
    return 2;
  }

  @Override
  public Class<GenericMatcher<IT1, IT2, OT>> getStubType() {
    @SuppressWarnings("unchecked")
    final Class<GenericMatcher<IT1, IT2, OT>> clazz =
        (Class<GenericMatcher<IT1, IT2, OT>>) (Class<?>) GenericMatcher.class;
    return clazz;
  }

  @Override
  public boolean requiresComparatorOnInput() {
    return true;
  }

  @Override
  public void prepare() throws Exception {

    if (!firstIteration) {
      return;
    }

    collector = new UpdateSolutionsetOutputCollector(taskContext.getOutputCollector());
  }

  @Override
  public void run() throws Exception {

    final GenericMatcher<IT1, IT2, OT> matchStub = taskContext.getStub();
    //TODO type safety
    final UpdateSolutionsetOutputCollector<OT> collector = this.collector;
    final MutableObjectIterator<IT1> probeSide = taskContext.getInput(0);
    final MutableObjectIterator<IT2> buildSide = taskContext.getInput(1);

    final MutableHashTable<IT2, IT1> hashJoin = Preconditions.checkNotNull(this.hashJoin);

    if (firstIteration) {
      hashJoin.open(buildSide, EmptyMutableObjectIterator.<IT1>get());
    }

    final IT1 probeSideRecord = taskContext.<IT1>getInputSerializer(0).createInstance();
    final IT2 buildSideRecord = taskContext.<IT2>getInputSerializer(1).createInstance();

    long possibleUpdates = 0;
    while (running && probeSide.next(probeSideRecord)) {
      MutableHashTable.HashBucketIterator<IT2, IT1> bucket = hashJoin.getMatchesFor(probeSideRecord);

      boolean matched = bucket.next(buildSideRecord);
      if (!matched) {
        throw new IllegalStateException("Unknown record supplied to solutionset");
      }
      collector.setHashBucket(bucket);

      matchStub.match(probeSideRecord, buildSideRecord, collector);
      possibleUpdates++;
    }

    long numUpdatedElements = collector.getNumUpdatedElementsAndReset();
    int workerIndex = taskContext.getOwningNepheleTask().getIndexInSubtaskGroup();

    if (log.isInfoEnabled()) {
      log.info("[" + numUpdatedElements + "] elements updated in the solutionset partition of worker " +
          "[" + workerIndex + "], possible updates [" + possibleUpdates + "]");
    }

    IterationContext.instance().setCount(workerIndex, numUpdatedElements);
  }

  @Override
  public void cleanup() throws Exception {
    firstIteration = false;
  }

  @Override
  public void cancel() {
    running = false;
    if (hashJoin != null) {
      hashJoin.close();
    }
  }

}
