package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.task.AbstractPactTask;

public abstract class AbstractIterativeTask extends AbstractPactTask {

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
  public void run() throws Exception {
    MutableObjectIterator<Value> input = inputs[0];
    ChannelStateTracker stateListener = stateListeners[0];

    IterationIterator iterationIter = new IterationIterator(input, stateListener);
    while (!iterationIter.checkTermination()) {
      //Send iterative open state to output gates
      StateUtils.publishState(ChannelState.OPEN, iterStateGates);

      //Call iteration stub function with the data for this iteration
      runIteration(iterationIter);

      if (stateListener.getState() == ChannelState.CLOSED) {
        StateUtils.publishState(ChannelState.CLOSED, iterStateGates);
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

  public abstract void runIteration(IterationIterator iterationIter) throws Exception;

}
