package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;

public abstract class AbstractIterativeTask extends AbstractMinimalTask {

  protected ChannelStateTracker[] stateListeners;
  protected OutputGate<? extends Record>[] iterStateGates;

  @SuppressWarnings("unchecked")
  @Override
  protected final void initInternal() {
    int numInputs = getNumberOfInputs();
    stateListeners = new ChannelStateTracker[numInputs];

    for (int i = 0; i < numInputs; i++) {
      stateListeners[i] =
          initStateTracking((InputGate<PactRecord>) getEnvironment().getInputGate(i));
    }

    iterStateGates = getIterationOutputGates();
  }

  @Override
  public void run() throws Exception {
    MutableObjectIterator<Value> input = inputs[0];
    ChannelStateTracker stateListener = stateListeners[0];

//    boolean firstRound = true;

    IterationIterator iterationIter = new IterationIterator(input, stateListener);
    while (!iterationIter.checkTermination()) {
      //Send iterative open state to output gates
      publishState(ChannelState.OPEN, iterStateGates);

//      if (firstRound) {
//        invokeStart();
//        firstRound = false;
//      }

      //Call iteration stub function with the data for this iteration
      runIteration(iterationIter);

      if (stateListener.getState() == ChannelState.CLOSED) {
        publishState(ChannelState.CLOSED, iterStateGates);
      } else {
        throw new RuntimeException("Illegal state after iteration call");
      }
    }
  }

  private OutputGate<? extends Record>[] getIterationOutputGates() {
    int numIterOutputs = this.config.getNumOutputs();

    @SuppressWarnings("unchecked")
    OutputGate<? extends Record>[] gates = new OutputGate[numIterOutputs];
    for (int i = 0; i < numIterOutputs; i++) {
      gates[i]  = getEnvironment().getOutputGate(i);
    }

    return gates;
  }

//  /**
//   * Allows to run code that should only be run once in the beginning
//   * @throws Exception
//   */
//  public abstract void invokeStart() throws Exception;

  public abstract void runIteration(IterationIterator iterationIter) throws Exception;

  public static ChannelStateTracker initStateTracking(InputGate<PactRecord> gate) {
    ChannelStateTracker tracker = new ChannelStateTracker(gate.getNumberOfInputChannels());
    gate.subscribeToEvent(tracker, ChannelStateEvent.class);
    return tracker;
  }

  public static void publishState(ChannelState state, OutputGate<?> gate) throws Exception {
    gate.flush(); //So that all records are hopefully send in a seperate envelope from the event
    //Now when the event arrives, all records will be processed before the state change.
    gate.publishEvent(new ChannelStateEvent(state));
    gate.flush();
  }

  public static void publishState(ChannelState state,
      OutputGate<? extends Record>[] iterStateGates) throws Exception {
    for (OutputGate<? extends Record> gate : iterStateGates) {
      publishState(state, gate);
    }
  }
}
