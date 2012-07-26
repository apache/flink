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
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.PactRecordNepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  The task responsible for synchronizing all iteration heads, implemented as an {@link AbstractOutputTask}. This task will never see any data.
 *  In each superstep, it simply waits until it has receiced an {@link EndOfSuperstepEvent} from each head and will send back an
 *  {@link AllWorkersDoneEvent} to signal that the next superstep can begin.
 */
public class BulkIterationSynchronizationSinkTask extends AbstractOutputTask implements Terminable {

  private TaskConfig taskConfig;

  private InterruptingMutableObjectIterator<PactRecord> recordIterator;
  private MutableRecordReader<PactRecord> reader;

  private int numIterations = 1;

  private final AtomicBoolean terminated = new AtomicBoolean(false);

  // this task will never see any records, just events
  private static final PactRecord DUMMY = new PactRecord();

  private static final Log log = LogFactory.getLog(BulkIterationSynchronizationSinkTask.class);

  //TODO this duplicates code from AbstractIterativePactTask
  @Override
  public void registerInputOutput() {

    taskConfig = new TaskConfig(getTaskConfiguration());

    String name = getEnvironment().getTaskName() + " (" + (getEnvironment().getIndexInSubtaskGroup() + 1) + '/' +
        getEnvironment().getCurrentNumberOfSubtasks() + ")";
    int numberOfEventsUntilInterrupt = taskConfig.getNumberOfEventsUntilInterruptInIterativeGate(0);

    if (log.isInfoEnabled()) {
      log.info(formatLogString("wrapping input [0] with an interrupting iterator that waits " +
          "for [" + numberOfEventsUntilInterrupt + "] event(s)"));
    }

    reader = new MutableRecordReader<PactRecord>(this);
    recordIterator = new InterruptingMutableObjectIterator<PactRecord>(new PactRecordNepheleReaderIterator(reader,
        ReaderInterruptionBehaviors.FALSE_ON_INTERRUPT), numberOfEventsUntilInterrupt, name, this);

    reader.subscribeToEvent(recordIterator, EndOfSuperstepEvent.class);
    reader.subscribeToEvent(recordIterator, TerminationEvent.class);
  }

  @Override
  public boolean terminationRequested() {
    return terminated.get();
  }

  @Override
  public void requestTermination() {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("marked as terminated."));
    }
    terminated.set(true);
  }

  @Override
  public void invoke() throws Exception {

    while (!terminationRequested()) {

      if (log.isInfoEnabled()) {
        log.info(formatLogString("starting iteration [" + numIterations + "]"));
      }

      readInput();

      if (log.isInfoEnabled()) {
        log.info(formatLogString("finishing iteration [" + numIterations + "]"));
      }

      if (checkTerminationCriterion(numIterations)) {

        if (log.isInfoEnabled()) {
          log.info(formatLogString("signaling that all workers are to terminate in iteration [" + numIterations + "]"));
        }

        requestTermination();
        sendToAllWorkers(new TerminationEvent());
      } else {

        if (log.isInfoEnabled()) {
          log.info(formatLogString("signaling that all workers are done in iteration [" + numIterations + "]"));
        }

        sendToAllWorkers(new AllWorkersDoneEvent());
        numIterations++;
      }

    }
  }

  private boolean checkTerminationCriterion(int numIterations) {
    Preconditions.checkState(taskConfig.getNumberOfIterations() > 0);
    return taskConfig.getNumberOfIterations() == numIterations;
  }

  private void readInput() throws IOException {
    boolean recordFound;
    while (recordFound = recordIterator.next(DUMMY)) {
      if (recordFound) {
        throw new IllegalStateException("Synchronization task must not see any records!");
      }
    }
  }

  private void sendToAllWorkers(AbstractTaskEvent event) throws IOException, InterruptedException {
    reader.publishEvent(event);
  }

  //TODO remove duplicated code
  public String formatLogString(String message) {
    return RegularPactTask.constructLogString(message, getEnvironment().getTaskName(), this);
  }

}
