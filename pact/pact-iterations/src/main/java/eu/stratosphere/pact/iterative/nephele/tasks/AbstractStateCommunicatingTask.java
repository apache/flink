package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.runtime.task.AbstractPactTask;

/**
 * abstract class that wraps state communication logic
 */
abstract class AbstractStateCommunicatingTask extends AbstractPactTask {

  protected ChannelStateTracker initStateTracking(InputGate<PactRecord> gate) {
    ChannelStateTracker tracker = new ChannelStateTracker(gate.getNumberOfInputChannels());
    gate.subscribeToEvent(tracker, ChannelStateEvent.class);
    return tracker;
  }

  protected void publishState(ChannelStateEvent.ChannelState state, OutputGate<?> gate) throws Exception {
    gate.flush(); //So that all records are hopefully send in a seperate envelope from the event
    //Now when the event arrives, all records will be processed before the state change.
    gate.publishEvent(new ChannelStateEvent(state));
    gate.flush();
  }

  protected void publishState(ChannelStateEvent.ChannelState state, OutputGate<? extends Record>[] iterStateGates)
      throws Exception {
    for (OutputGate<? extends Record> gate : iterStateGates) {
      publishState(state, gate);
    }
  }
}
