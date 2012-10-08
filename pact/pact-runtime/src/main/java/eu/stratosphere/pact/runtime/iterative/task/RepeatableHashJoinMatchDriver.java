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
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.generic.GenericMatcher;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.plugable.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

import java.util.List;

public class RepeatableHashJoinMatchDriver<IT1, IT2, OT> implements PactDriver<GenericMatcher<IT1, IT2, OT>, OT> {

  protected PactTaskContext<GenericMatcher<IT1, IT2, OT>, OT> taskContext;

  private boolean firstIteration = true;

  //TODO typesafety
  private volatile MutableHashTable hashJoin;

  private volatile boolean running;

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

    TaskConfig config = taskContext.getTaskConfig();

    TypeSerializer<IT1> probesideSerializer = taskContext.getInputSerializer(0);
    TypeSerializer<IT2> buildsideSerializer = taskContext.getInputSerializer(1);
    TypeComparator<IT1> probesideComparator = taskContext.getInputComparator(0);
    TypeComparator<IT2> buildSideComparator = taskContext.getInputComparator(1);

    //TODO duplicated from MatchDriver, refactor
    final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory;
    try {
      final Class<? extends TypePairComparatorFactory<IT1, IT2>> factoryClass =
          config.getPairComparatorFactory(taskContext.getUserCodeClassLoader());

      if (factoryClass == null) {
        @SuppressWarnings("unchecked")
        TypePairComparatorFactory<IT1, IT2> pactRecordFactory =
            (TypePairComparatorFactory<IT1, IT2>) PactRecordPairComparatorFactory.get();
        pairComparatorFactory = pactRecordFactory;
      } else {
        @SuppressWarnings("unchecked")
        final Class<TypePairComparatorFactory<IT1, IT2>> clazz = (Class<TypePairComparatorFactory<IT1, IT2>>)
            (Class<?>) TypePairComparatorFactory.class;
        pairComparatorFactory = InstantiationUtil.instantiate(factoryClass, clazz);
      }
    } catch (ClassNotFoundException e) {
      throw new Exception("The class registered as TypePairComparatorFactory cloud not be loaded.", e);
    }

    List<MemorySegment> memSegments = taskContext.getMemoryManager().allocatePages(taskContext.getOwningNepheleTask(),
        config.getMemorySize());

    hashJoin = new MutableHashTable(buildsideSerializer, probesideSerializer, buildSideComparator,
        probesideComparator, pairComparatorFactory.createComparator12(probesideComparator, buildSideComparator),
        memSegments, taskContext.getIOManager());
  }

  @Override
  public void run() throws Exception {
    //TODO configure build and probeside index
    final GenericMatcher<IT1, IT2, OT> matchStub = taskContext.getStub();
    //TODO type safety
    final Collector<OT> collector = taskContext.getOutputCollector();
    final MutableObjectIterator<IT1> probeSide = taskContext.getInput(0);
    final MutableObjectIterator<IT2> buildSide = taskContext.getInput(1);

    final MutableHashTable<IT2, IT1> hashJoin = Preconditions.checkNotNull(this.hashJoin);

    if (firstIteration) {
      hashJoin.open(buildSide, EmptyMutableObjectIterator.<IT1>get());
    }

    final IT1 probeSideRecord = taskContext.<IT1>getInputSerializer(0).createInstance();
    final IT2 buildSideRecord = taskContext.<IT2>getInputSerializer(1).createInstance();

    while (running && probeSide.next(probeSideRecord)) {
      MutableHashTable.HashBucketIterator<IT2, IT1> bucket = hashJoin.getMatchesFor(probeSideRecord);
      while (bucket.next(buildSideRecord)) {
        matchStub.match(probeSideRecord, buildSideRecord, collector);
      }
    }
  }

  @Override
  public void cleanup() throws Exception {
    firstIteration = false;
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
