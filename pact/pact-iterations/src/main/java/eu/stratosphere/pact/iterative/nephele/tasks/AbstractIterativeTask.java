package eu.stratosphere.pact.iterative.nephele.tasks;

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;

public abstract class AbstractIterativeTask extends AbstractStateCommunicatingTask {

  protected ChannelStateTracker[] stateListeners;
  protected OutputGate<? extends Record>[] iterStateGates;

//  @SuppressWarnings("unchecked")
//  @Override
//  protected final void initInternal() {
//    int numInputs = getNumberOfInputs();
//    stateListeners = new ChannelStateTracker[numInputs];
//
//    for (int i = 0; i < numInputs; i++) {
//      stateListeners[i] = initStateTracking((InputGate<PactRecord>) getEnvironment().getInputGate(i));
//    }
//
//    iterStateGates = getIterationOutputGates();
//  }


  @Override
  public void prepare() throws Exception {
    System.out.println("Starting preparation of " + getClass().getSimpleName() + " with " + getNumberOfInputs() + " inputs");
    int numInputs = getNumberOfInputs();
    stateListeners = new ChannelStateTracker[numInputs];

    for (int i = 0; i < numInputs; i++) {
      stateListeners[i] = Preconditions.checkNotNull(
          initStateTracking((InputGate<PactRecord>) getEnvironment().getInputGate(i)));
    }

    iterStateGates = getIterationOutputGates();
  }

  @Override
  public void run() throws Exception {

    Preconditions.checkNotNull(inputs, "Inputs cannot be null");
    Preconditions.checkNotNull(stateListeners, "stateListeners cannot be null");
    Preconditions.checkState(inputs.length > 0 && stateListeners.length == inputs.length,
        "Must have at least one input");

    MutableObjectIterator<Value> input = inputs[0];
    ChannelStateTracker stateListener = stateListeners[0];

    IterationIterator iterationIter = new IterationIterator(input, stateListener);
    while (!iterationIter.checkTermination()) {
      //Send iterative open state to output gates
      publishState(ChannelState.OPEN, iterStateGates);

      //Call iteration stub function with the data for this iteration
      runIteration(iterationIter);

      Preconditions.checkState(stateListener.getState() == ChannelState.CLOSED, "Illegal state after iteration call");

      publishState(ChannelState.CLOSED, iterStateGates);

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

  public abstract void runIteration(IterationIterator iterationIter) throws Exception;

}
