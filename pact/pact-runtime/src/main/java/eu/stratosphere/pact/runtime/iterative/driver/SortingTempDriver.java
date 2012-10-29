package eu.stratosphere.pact.runtime.iterative.driver;

/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.pact.runtime.sort.Sorter;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class SortingTempDriver<T> implements PactDriver<Stub, T>
{
  private static final Log LOG = LogFactory.getLog(SortingTempDriver.class);

  private static final long MIN_REQUIRED_MEMORY = 512 * 1024;		// minimal memory for the task to operate

  private PactTaskContext<Stub, T> taskContext;

  private Sorter<T> sorter;

  private volatile boolean running;

  // ------------------------------------------------------------------------


  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.PactDriver#setup(eu.stratosphere.pact.runtime.task.PactTaskContext)
    */
  @Override
  public void setup(PactTaskContext<Stub, T> context) {
    this.taskContext = context;
    this.running = true;
  }

  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
    */
  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
    */
  @Override
  public Class<Stub> getStubType() {
    return Stub.class;
  }

  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
    */
  @Override
  public boolean requiresComparatorOnInput() {
    return true;
  }

  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
    */
  @Override
  public void prepare() throws Exception
  {
    final TaskConfig config = this.taskContext.getTaskConfig();

    // set up memory and I/O parameters
    final long availableMemory = config.getMemorySize();

    if (availableMemory < MIN_REQUIRED_MEMORY) {
      throw new RuntimeException("The temp task was initialized with too little memory: " + availableMemory +
          ". Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
    }

    sorter = new UnilateralSortMerger(taskContext.getMemoryManager(), taskContext.getIOManager(),
        taskContext.getInput(0), taskContext.getOwningNepheleTask(), taskContext.getInputSerializer(0),
        taskContext.getInputComparator(0), availableMemory, config.getNumFilehandles(),
        config.getSortSpillingTreshold());
  }


  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
    */
  @Override
  public void run() throws Exception
  {
    if (LOG.isDebugEnabled()) {
      LOG.debug(this.taskContext.formatLogString("Preprocessing done, iterator obtained."));
    }

    // cache references on the stack
    final TypeSerializer<T> serializer = this.taskContext.getInputSerializer(0);
    final Collector<T> output = this.taskContext.getOutputCollector();

    final T record = serializer.createInstance();
    MutableObjectIterator<T> sorted = sorter.getIterator();

    while (this.running && sorted.next(record)) {
      output.collect(record);
    }

  }

  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
    */
  @Override
  public void cleanup() throws Exception {
  }

  /* (non-Javadoc)
    * @see eu.stratosphere.pact.runtime.task.PactDriver#cancel()
    */
  @Override
  public void cancel() {
    this.running = false;
  }
}

