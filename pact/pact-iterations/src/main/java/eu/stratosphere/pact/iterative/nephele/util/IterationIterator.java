package eu.stratosphere.pact.iterative.nephele.util;

import java.io.IOException;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public class IterationIterator implements MutableObjectIterator<Value> {

  private MutableObjectIterator<Value> iter;
  private ChannelStateTracker listener;
  private PactRecord dummyRecord;
  private boolean closed = true;

  public IterationIterator(MutableObjectIterator<Value> input, ChannelStateTracker listener) {
    this.iter = input;
    this.listener = listener;
    this.dummyRecord = new PactRecord();
  }

  @Override
  public boolean next(Value target) throws IOException {
    if (closed) {
      return false;
    }

    //Needs to be in a loop, because multiple events can come between records
    //(every event needs one loop because of the exceptions)
    while (true) {
      try {
        boolean success = iter.next(target);

        if (!success) {
          //Channel should be closed before it is exhausted
          throw new RuntimeException("Channel is not closed but exhausted");
        }
        return success;
      } catch (StateChangeException ex) {
        //When this function is called the current state always has to be ChannelState.OPENED
        //this is assured by first calling checkTermination(). So the only valid state change
        //is from open to closed.
        if (listener.isChanged()) {
          ChannelState state = listener.getState();
          if (state == ChannelState.CLOSED) {
            closed = true;
            return false;
          } else {
            throw new RuntimeException("Should never happen");
          }
        }
      }
    }
  }

  /**
   * Checks whether the input is terminate. If false the state is open
   * @return
   * @throws IOException
   */
  public boolean checkTermination() throws IOException {
    while (true) {
      try {
        boolean success = iter.next(dummyRecord);
        if (success) {
          //When calling this function the ChannelState should be closed and the next epected
          //thing is a event and not a record
          throw new RuntimeException("Record received but only events were expected");
        } else {
          //If it returned without access the channel was closed => terminated
          return true;
        }
      } catch (StateChangeException ex) {
        if (listener.isChanged()) {
          if (listener.getState() == ChannelState.OPEN) {
            //Channel is opened so it can't be terminated
            closed = false;
            return false;
          } else {
            throw new RuntimeException("Only expected state change is to open" + listener.getState());
          }
        }
      }
    }
  }
}
