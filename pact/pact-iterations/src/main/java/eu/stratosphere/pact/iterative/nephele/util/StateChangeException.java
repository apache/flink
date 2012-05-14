package eu.stratosphere.pact.iterative.nephele.util;

import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

@SuppressWarnings("serial")
public class StateChangeException extends RuntimeException {

  ChannelState state;
  public StateChangeException(ChannelState state) {
    this.state = state;
  }

  @Override
  public String getMessage() {
    return state.toString();
  }
}
