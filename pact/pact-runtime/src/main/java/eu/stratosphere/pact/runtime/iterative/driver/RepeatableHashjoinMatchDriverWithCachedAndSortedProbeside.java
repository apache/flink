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


import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.TypeUtils;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.sort.Sorter;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.ResettableMutableObjectIterator;

public class RepeatableHashjoinMatchDriverWithCachedAndSortedProbeside<IT1, IT2, OT>
    extends AbstractRepeatableMatchDriver {

  private ResettableMutableObjectIterator<IT1> cachedProbeSide;

  @Override
  protected long getMemorySizeForHashjoin() {
    TaskConfig config = taskContext.getTaskConfig();
    long completeMemorySize = config.getMemorySize();
    long hashJoinMemorySize = (long) (completeMemorySize * 0.25);
    config.setMemorySize(completeMemorySize - hashJoinMemorySize);
    return hashJoinMemorySize;
  }

  @Override
  protected void beforeIteration(MutableObjectIterator probeSide, MutableObjectIterator buildSide) throws Exception {

    hashJoin.open(buildSide, EmptyMutableObjectIterator.<IT1>get());

    if (isFirstIteration()) {
      TaskConfig config = taskContext.getTaskConfig();

      TypeSerializer<IT1> probeSideSerializer = taskContext.getInputSerializer(0);
      MemoryManager memoryManager = taskContext.getMemoryManager();
      IOManager ioManager = taskContext.getIOManager();
      AbstractInvokable owningNepheleTask = taskContext.getOwningNepheleTask();
      ClassLoader userCodeClassLoader = taskContext.getUserCodeClassLoader();

      long completeMemorySize = config.getMemorySize();
      long sorterMemorySize = (long) (completeMemorySize * 0.25);
      config.setMemorySize(completeMemorySize - sorterMemorySize);

      TypeComparator<IT1> probesideComparator = TypeUtils.instantiateTypeComparator(config.getConfiguration(),
          userCodeClassLoader, config.getCachedHashJoinProbeSideComparatorFactoryClass(userCodeClassLoader),
          config.getCachedHashjoinProbesideComparatorPrefix());

      Sorter<IT1> sorter = new UnilateralSortMerger(memoryManager, ioManager, probeSide, owningNepheleTask,
          probeSideSerializer, probesideComparator, sorterMemorySize, config.getNumFilehandles(),
          config.getSortSpillingTreshold());

      cachedProbeSide = new SpillingResettableMutableObjectIterator<IT1>(sorter.getIterator(), probeSideSerializer,
          memoryManager, ioManager, config.getMemorySize(), owningNepheleTask);

    } else {
      cachedProbeSide.reset();
    }
  }

  @Override
  protected MutableObjectIterator getProbeSideForMatch() {
    return cachedProbeSide;
  }

  @Override
  protected void afterIteration() {
    hashJoin.close();
  }
}
