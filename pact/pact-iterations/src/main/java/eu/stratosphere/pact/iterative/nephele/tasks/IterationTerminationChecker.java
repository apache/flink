package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.samples.DoNothingStub;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.StateChangeException;
import eu.stratosphere.pact.iterative.nephele.util.TerminationDecider;

public class IterationTerminationChecker extends AbstractStateCommunicatingTask {

  public static final String TERMINATION_DECIDER = "iter.termination.decider";

  private TerminationDecider decider = null;
  private ChannelStateTracker[] stateListeners;

  @Override
  public void run() throws Exception {
    MutableObjectIterator<Value> input = inputs[0];
    ChannelStateTracker stateListener = stateListeners[0];

    List<PactRecord> values = new ArrayList<PactRecord>();
    while (true) {
      try {
        PactRecord rec = new PactRecord();
        boolean success = input.next(rec);
        if (success) {
          values.add(rec);
        } else {
          //If it returned, but there is no state change the iterator is exhausted
          // => Finishing
          break;
        }
      } catch (StateChangeException ex) {
        if (stateListener.isChanged() && stateListener.getState() == ChannelState.CLOSED) {
          //Ask oracle whether to stop the program
          boolean terminate = decider.decide(values.iterator());
          values.clear();

          if (terminate) {
            getEnvironment().getInputGate(1).publishEvent(new ChannelStateEvent(ChannelState.TERMINATED));
          } else {
            getEnvironment().getInputGate(1).publishEvent(new ChannelStateEvent(ChannelState.CLOSED));
          }
        }
      }
    }

    output.close();
  }

  @Override
  public int getNumberOfInputs() {
    return 2;
  }

  @Override
  public void cleanup() throws Exception {}

  @Override
  public boolean requiresComparatorOnInput() {
    //TODO implement
    return false;
  }

  @Override
  public void prepare() throws Exception {
    Class<?> cls = getTaskConfiguration().getClass(TERMINATION_DECIDER, null);
    try {
      decider = (TerminationDecider) cls.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException("Could not instantiate termination decider", ex);
    }

    int numInputs = getNumberOfInputs();
    stateListeners = new ChannelStateTracker[numInputs];

    for (int i = 0; i < numInputs; i++) {
      stateListeners[i] = initStateTracking((InputGate<PactRecord>) getEnvironment().getInputGate(i));
    }
  }
}
