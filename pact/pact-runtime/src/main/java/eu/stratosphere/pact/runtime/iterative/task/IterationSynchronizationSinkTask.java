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
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;
import eu.stratosphere.pact.runtime.iterative.convergence.ConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
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
 *  In each superstep, it simply waits until it has receiced a {@link WorkerDoneEvent} from each head and will send back an
 *  {@link AllWorkersDoneEvent} to signal that the next superstep can begin.
 */
public class IterationSynchronizationSinkTask extends AbstractOutputTask implements Terminable {

  private TaskConfig taskConfig;

  private InterruptingMutableObjectIterator<PactRecord> headEventRecordIterator;
  private MutableRecordReader<PactRecord> headEventReader;

  //TODO typesafety
  private ConvergenceCriterion convergenceCriterion;
  private Aggregator aggregator;

  private int currentIteration = 1;

  private final AtomicBoolean terminated = new AtomicBoolean(false);

  private final PactRecord headEventRecord = new PactRecord();

  private static final Log log = LogFactory.getLog(IterationSynchronizationSinkTask.class);

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

    if (taskConfig.usesConvergenceCriterion()) {
      convergenceCriterion = InstantiationUtil.instantiate(taskConfig.getConvergenceCriterion(),
          ConvergenceCriterion.class);
      aggregator = convergenceCriterion.createAggregator();
    }

    headEventReader = new MutableRecordReader<PactRecord>(this, 0);
    headEventRecordIterator = new InterruptingMutableObjectIterator<PactRecord>(
        new PactRecordNepheleReaderIterator(headEventReader, ReaderInterruptionBehaviors.RELEASE_ON_INTERRUPT),
        numberOfEventsUntilInterrupt, name, this, 0, aggregator);

    headEventReader.subscribeToEvent(headEventRecordIterator, WorkerDoneEvent.class);
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

      notifyMonitor(IterationMonitoring.Event.SYNC_STARTING, currentIteration);
      if (log.isInfoEnabled()) {
        log.info(formatLogString("starting iteration [" + currentIteration + "]"));
      }

      readHeadEventChannel();

      if (log.isInfoEnabled()) {
        log.info(formatLogString("finishing iteration [" + currentIteration + "]"));
      }

      if (checkForConvergence()) {

        if (log.isInfoEnabled()) {
          log.info(formatLogString("signaling that all workers are to terminate in iteration [" + currentIteration +
              "]"));
        }

        requestTermination();
        sendToAllWorkers(new TerminationEvent());
        notifyMonitor(IterationMonitoring.Event.SYNC_FINISHED, currentIteration);
      } else {

        if (log.isInfoEnabled()) {
          log.info(formatLogString("signaling that all workers are done in iteration [" + currentIteration + "]"));
        }

        sendToAllWorkers(new AllWorkersDoneEvent());
        notifyMonitor(IterationMonitoring.Event.SYNC_FINISHED, currentIteration);
        currentIteration++;
      }

    }
  }

  protected void notifyMonitor(IterationMonitoring.Event event, int currentIteration) {
    if (log.isInfoEnabled()) {
      log.info(IterationMonitoring.logLine(getEnvironment().getJobID(), event, currentIteration, 1));
    }
  }

  private boolean checkForConvergence() throws IOException, InterruptedException {
    Preconditions.checkState(taskConfig.getNumberOfIterations() > 0);

    if (taskConfig.usesConvergenceCriterion()) {

      Value aggregate = aggregator.getAggregate();
      aggregator.reset();

      if (convergenceCriterion.isConverged(currentIteration, aggregate)) {
        if (log.isInfoEnabled()) {
          log.info(formatLogString("convergence reached after [" + currentIteration + "] iterations, terminating..."));
        }
        return true;
      }
    }

    if (taskConfig.getNumberOfIterations() == currentIteration) {
      if (log.isInfoEnabled()) {
        log.info(formatLogString("maximum number of iterations [" + currentIteration + "] reached, terminating..."));
      }
      return true;
    }

    return false;
  }

  private void readHeadEventChannel() throws IOException {
    while (headEventRecordIterator.next(headEventRecord)) {
      throw new IllegalStateException("Synchronization task must not see any records!");
    }
  }

  private void sendToAllWorkers(AbstractTaskEvent event) throws IOException, InterruptedException {
    headEventReader.publishEvent(event);
  }

  //TODO remove duplicated code
  public String formatLogString(String message) {
    return RegularPactTask.constructLogString(message, getEnvironment().getTaskName(), this);
  }

}
