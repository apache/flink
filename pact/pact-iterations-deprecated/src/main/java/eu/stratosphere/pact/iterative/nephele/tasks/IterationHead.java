package eu.stratosphere.pact.iterative.nephele.tasks;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.iterative.nephele.util.DeserializingIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.PactRecordOutputCollector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.BackTrafficQueueStore;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.SerializedUpdateBuffer;

public abstract class IterationHead extends AbstractStateCommunicatingTask {

  protected static final Log LOG = LogFactory.getLog(IterationHead.class);
  protected static final int MEMORY_SEGMENT_SIZE = 1024 * 1024;

  public static final String FIXED_POINT_TERMINATOR = "pact.iter.fixedpoint";
  public static final String NUMBER_OF_ITERATIONS = "pact.iter.numiterations";

  protected ClosedListener channelStateListener;
  protected ClosedListener terminationStateListener;
  protected int numInternalOutputs;
  protected long updateBufferSize = -1;

  protected volatile boolean finished = false;

  protected PactRecordOutputCollector outputCollector;

  @Override
  public void prepare() throws Exception {
    boolean useFixedPointTerminator = getEnvironment().getTaskConfiguration()
        .getBoolean(FIXED_POINT_TERMINATOR, false);

    channelStateListener = new ClosedListener();
    getEnvironment().getOutputGate(2).subscribeToEvent(channelStateListener, ChannelStateEvent.class);

    if (useFixedPointTerminator) {
      terminationStateListener = new ClosedListener();
      getEnvironment().getOutputGate(4).subscribeToEvent(terminationStateListener, ChannelStateEvent.class);
      numInternalOutputs = 5;
    } else {
      int numIterations = getEnvironment().getTaskConfiguration().getInteger(NUMBER_OF_ITERATIONS, -1);
      terminationStateListener = new FixedRoundListener(numIterations);
      numInternalOutputs = 4;
    }

    updateBufferSize = config.getMemorySize() * 1 / 5;
    config.setMemorySize(config.getMemorySize() * 4 / 5);

    //TODO refactor the collectors
    Preconditions.checkState(output instanceof PactRecordOutputCollector);
    outputCollector = (PactRecordOutputCollector) output;
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @Override
  public void run() throws Exception {

    //Setup variables for easier access to the correct output gates / writers
    //Create output collector for intermediate results
    PactRecordOutputCollector innerOutput = new PactRecordOutputCollector(Arrays.asList(getIterationRecordWriters()));

    //Create output collector for final iteration output
    PactRecordOutputCollector taskOutput = new PactRecordOutputCollector(Arrays.asList(outputCollector.getWriter(0)));

    //Gates where the iterative channel state is send to
    OutputGate<? extends Record>[] iterStateGates = getIterationOutputGates();

    //Allocate memory for update queue
    LOG.info("Update memory: " + updateBufferSize + ", numSegments: " + updateBufferSize);
//    List<MemorySegment> updateMemory = getEnvironment().getMemoryManager().allocateStrict(this,
//            (int) (updateBufferSize / MEMORY_SEGMENT_SIZE), MEMORY_SEGMENT_SIZE);
    List<MemorySegment> updateMemory = getEnvironment().getMemoryManager().allocatePages(this, updateBufferSize);

      SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(updateMemory, MEMORY_SEGMENT_SIZE,
        getEnvironment().getIOManager());

    //Create and initialize internal structures for the transport of the iteration updates from the tail to the head (this class)
    BackTrafficQueueStore.getInstance().addStructures(getEnvironment().getJobID(),
        getEnvironment().getIndexInSubtaskGroup());
    BackTrafficQueueStore.getInstance().publishUpdateBuffer(getEnvironment().getJobID(),
        getEnvironment().getIndexInSubtaskGroup(), buffer);

    //Start with a first iteration run using the input data
    publishState(ChannelState.OPEN, iterStateGates);

    if (LOG.isInfoEnabled()) {
      LOG.info(constructLogString("Starting Iteration: -1", getEnvironment().getTaskName(), this));
    }
    //Process all input records by passing them to the processInput method (supplied by the user)
    MutableObjectIterator<PactRecord> input = getInput(0);
    processInput(input, innerOutput);

    //Send iterative close event to indicate that this round is finished
//    sendCounter("iter.received.messages", statsIter.getCount());
//    sendCounter("iter.send.messages", statsOutputCollector.getCount());
    publishState(ChannelState.CLOSED, iterStateGates);
    //AbstractIterativeTask.publishState(ChannelState.CLOSED, terminationOutputGate);

    //Loop until iteration terminates
    int iterationCounter = 0;
    SerializedUpdateBuffer updatesBuffer;
    while (true) {
      //Wait until previous iteration run is finished for this subtask
      //and retrieve buffered updates
      try {
        updatesBuffer = (SerializedUpdateBuffer) BackTrafficQueueStore.getInstance().receiveIterationEnd(
            getEnvironment().getJobID(), getEnvironment().getIndexInSubtaskGroup());
      } catch (InterruptedException ex) {
        throw new RuntimeException("Internal Error", ex);
      }

      //Wait until also other parallel subtasks have terminated and a termination decision has been made
      try {
        channelStateListener.waitForUpdate();
        terminationStateListener.waitForUpdate();
      } catch (InterruptedException e) {
        throw new RuntimeException("Internal Error", e);
      }


      if (channelStateListener.isUpdated() && terminationStateListener.isUpdated()) {
        //Check termination criterion
        if (terminationStateListener.getState() == ChannelState.TERMINATED) {
          break;
        } else {
          if (LOG.isInfoEnabled()) {
            LOG.info(constructLogString("Starting Iteration: " + iterationCounter, getEnvironment().getTaskName(), this));
          }
          input = new DeserializingIterator(updatesBuffer.switchBuffers());

          BackTrafficQueueStore.getInstance().publishUpdateBuffer(getEnvironment().getJobID(),
              getEnvironment().getIndexInSubtaskGroup(), buffer);

          //Start new iteration run
          publishState(ChannelState.OPEN, iterStateGates);

          //Call stub function to process updates
          processUpdates(input, innerOutput);

//          sendCounter("iter.received.messages", statsIter.getCount());
//          sendCounter("iter.send.messages", statsOutputCollector.getCount());
          publishState(ChannelState.CLOSED, iterStateGates);

          updatesBuffer = null;
          iterationCounter++;
        }
      } else {
        throw new RuntimeException("isUpdated() returned false even thoug waitForUpate() exited");
      }
    }

    //Call stub so that it can finish its code
    finish(new DeserializingIterator(updatesBuffer.switchBuffers()), taskOutput);

    //Release the structures for this iteration
    if (updatesBuffer != null) {
      //TODO: Deactivated so that broadcast job finished
      //updatesBuffer.close();
    }
    getEnvironment().getMemoryManager().release(updateMemory);

    finished = true;
  }

  @Override
  public void cleanup() throws Exception {
    BackTrafficQueueStore.getInstance().releaseStructures(getEnvironment().getJobID(),
        getEnvironment().getIndexInSubtaskGroup());
  }

  public abstract void finish(MutableObjectIterator<PactRecord> iter, PactRecordOutputCollector output) throws Exception;

  public abstract void processInput(MutableObjectIterator<PactRecord> iter, PactRecordOutputCollector output) throws Exception;

  public abstract void processUpdates(MutableObjectIterator<PactRecord> iter, PactRecordOutputCollector output) throws Exception;

  public static String constructLogString(String message, String taskName, AbstractInvokable parent) {
    StringBuilder bld = new StringBuilder(128);
    bld.append(message);
    bld.append(':').append(' ');
    bld.append(taskName);
    bld.append(' ').append('(');
    bld.append(parent.getEnvironment().getIndexInSubtaskGroup() + 1);
    bld.append('/');
    bld.append(parent.getEnvironment().getCurrentNumberOfSubtasks());
    bld.append(')');
    return bld.toString();
  }

  protected AbstractRecordWriter<PactRecord>[] getIterationRecordWriters() {
    int numIterOutputs = config.getNumOutputs() - numInternalOutputs;

    @SuppressWarnings("unchecked")
    AbstractRecordWriter<PactRecord>[] writers = new AbstractRecordWriter[numIterOutputs];
    for (int i = 0; i < numIterOutputs; i++) {
      writers[i]  = outputCollector.getWriter(numInternalOutputs + i);
    }

    return writers;
  }

  protected OutputGate<? extends Record>[] getIterationOutputGates() {
    int numIterOutputs = this.config.getNumOutputs() - numInternalOutputs;

    @SuppressWarnings("unchecked")
    OutputGate<? extends Record>[] gates = new OutputGate[numIterOutputs + 1];
    gates[0] = getEnvironment().getOutputGate(3);
    for (int i = 0; i < numIterOutputs; i++) {
      gates[1 + i]  = getEnvironment().getOutputGate(numInternalOutputs + i);
    }

    return gates;
  }

  PactRecord countRec = new PactRecord();
  PactString keyStr = new PactString();
  PactLong countLng = new PactLong();

  protected void sendCounter(String key, long count) throws IOException, InterruptedException {
    keyStr.setValue(key);
    countLng.setValue(count);
    countRec.setField(0, keyStr);
    countRec.setField(1, countLng);
    //TODO introduce constant
    outputCollector.getWriter(3).emit(countRec);
  }

  protected class ClosedListener implements EventListener {
    final Object object = new Object();
    volatile ChannelState state;
    volatile boolean updated = false;

    @Override
    public void eventOccurred(AbstractTaskEvent event) {
      if (!finished) {
        synchronized(object) {
          updated = true;
          state = ((ChannelStateEvent) event).getState();
          if (state != ChannelState.CLOSED  && state != ChannelState.TERMINATED) {
            throw new RuntimeException("Impossible");
          }
          object.notifyAll();
        }
      }
    }

    public void waitForUpdate() throws InterruptedException {
      synchronized (object) {
        while (!updated) {
          object.wait();
        }
      }
    }

    public boolean isUpdated() {
      if (updated) {
        updated = false;
        return true;
      }

      return false;
    }

    public ChannelState getState() {
      return state;
    }
  }

  protected class FixedRoundListener extends ClosedListener {
    int numRounds;
    int currentRound;

    public FixedRoundListener(int numIterations) {
      this.numRounds = numIterations;
      this.currentRound = 0;
    }

    @Override
    public void waitForUpdate() throws InterruptedException {
      return;
    }

    @Override
    public boolean isUpdated() {
      return true;
    }

    @Override
    public ChannelState getState() {
      currentRound++;
      if (currentRound == numRounds) {
        return ChannelState.TERMINATED;
      } else {
        return ChannelState.OPEN;
      }
    }
  }

  protected static class CountingIterator implements MutableObjectIterator<Value> {

    private MutableObjectIterator<Value> iter;
    private boolean first = true;
    private long count;

    public CountingIterator(MutableObjectIterator<Value> iter) {
      this.iter = iter;
    }

    @Override
    public boolean next(Value target) throws IOException {
      boolean success = iter.next(target);

      if (success) {
        if (first) {
          first = false;
        }
        count++;
      }

      return success;
    }

    public long getCount() {
      return count;
    }
  }

  protected static class CountingOutputCollector<T> implements Collector<T> {

    private Collector collector;
    private long counter;

    public CountingOutputCollector(Collector<T> output) {
      super();
      collector = output;
    }

    @Override
    public void collect(T record) {
      counter++;
      collector.collect(record);
    }

    @Override
    public void close() {
      collector.close();
    }

    public long getCount() {
      return counter;
    }
  }
}
