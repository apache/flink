package eu.stratosphere.pact.runtime.iterative.task;

import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.PactRecordNepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkIterationSynchronizationSink extends AbstractOutputTask implements Terminable {

  private TaskConfig taskConfig;

  private MutableObjectIterator<PactRecord> recordIterator;
  private MutableRecordReader<PactRecord> reader;

  private int numIterations;

  private final AtomicBoolean terminated = new AtomicBoolean(false);

  // this task will never see any records, just events
  private static final PactRecord DUMMY = new PactRecord();

  private static final Log log = LogFactory.getLog(BulkIterationSynchronizationSink.class);

  @Override
  public void registerInputOutput() {

    String name = getEnvironment().getTaskName() + " (" + (getEnvironment().getIndexInSubtaskGroup() + 1) + '/' +
        getEnvironment().getCurrentNumberOfSubtasks() + ")";
    int numberOfEventsUntilInterrupt = taskConfig.getNumberOfEventsUntilInterruptInIterativeGate(0);

    reader = new MutableRecordReader<PactRecord>(this);
    recordIterator = new InterruptingMutableObjectIterator<PactRecord>(new PactRecordNepheleReaderIterator(reader,
        ReaderInterruptionBehaviors.FALSE_ON_INTERRUPT), numberOfEventsUntilInterrupt, name, this);
  }

  @Override
  public boolean isTerminated() {
    return terminated.get();
  }

  @Override
  public void terminate() {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("marked as terminated."));
    }
    terminated.set(true);
  }

  @Override
  public void invoke() throws Exception {

    while (!isTerminated()) {

      if (log.isInfoEnabled()) {
        log.info(formatLogString("starting iteration [" + numIterations + "]"));
      }

      readInput();

      if (log.isInfoEnabled()) {
        log.info(formatLogString("finishing iteration [" + numIterations + "]"));
      }

      if (!isTerminated()) {
        if (log.isInfoEnabled()) {
          log.info(formatLogString("signaling that all workers are done in iteration [" + numIterations + "]"));
        }

        signalAllWorkersDone();
      }

      numIterations++;
    }

  }

  private void readInput() throws IOException {
    boolean recordFound;
    while (recordFound = recordIterator.next(DUMMY)) {
      if (recordFound) {
        throw new IllegalStateException("Synchronization task shall never see any records!");
      }
    }
  }

  private void signalAllWorkersDone() throws IOException, InterruptedException {
    reader.publishEvent(new AllWorkersDoneEvent());
  }

  //TODO remove duplicated code
  public String formatLogString(String message) {
    return RegularPactTask.constructLogString(message, getEnvironment().getTaskName(), this);
  }

}
