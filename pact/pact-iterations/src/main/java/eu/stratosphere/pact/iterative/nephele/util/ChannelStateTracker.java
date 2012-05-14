package eu.stratosphere.pact.iterative.nephele.util;

import java.util.LinkedList;
import java.util.Queue;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

/**
 * This class tracks the channel state concerning the internal iteration state.
 * It makes sure that only valid state transitions are made and if there are
 * multiple preceding tasks it only changes the final state when all preceding
 * subtasks send the same state.
 * 
 * @author mkaufmann
 *
 */
public class ChannelStateTracker implements EventListener {
	private volatile ChannelState state = ChannelState.STARTED;
	private volatile ChannelState nextState = null;
	private volatile Queue<ChannelState> waitingStates;
	
	private final int numChannels;
	private volatile int nextStateCount = 0;
	private volatile boolean stateChanged = false;
	
	public ChannelStateTracker(int numChannels) {
		this.numChannels = numChannels;
		this.waitingStates = new LinkedList<ChannelState>();
	}
	
	@Override
	public void eventOccurred(AbstractTaskEvent event) {
		ChannelStateEvent evt = (ChannelStateEvent) event;
		ChannelState evtState = evt.getState();
		
		if(nextState == null) {
			nextState = evtState;
			if(nextState == ChannelState.OPEN) {
				state = nextState;
				stateChanged = true;
			}
			nextStateCount = 1;
		} else if(nextState == evtState) {
			nextStateCount++;
		} else {
			//It can happen, that some channels already send closed events while others
			//still have to send open events
			if(nextState == ChannelState.OPEN && evtState == ChannelState.CLOSED) {
				waitingStates.add(evtState);
			} else {
				throw new RuntimeException("Expected next state is " + nextState + " but channel changed to " + state);
			}
		}
		
		//Check if all channels have read the next state
		if(nextStateCount == numChannels) {
			state = nextState;
			
			//Handle deferred close events
			if(waitingStates.isEmpty()) {
				nextState = null;
			} else {
				//Process waiting close events, it is guaranteed that only CLOSED events are in
				//the list
				nextStateCount = waitingStates.size();
				nextState = waitingStates.poll();
				if(nextState != ChannelStateEvent.ChannelState.CLOSED) {
					throw new RuntimeException("Impossible ");
				}
				waitingStates.clear();				
			}
			
			if(state != ChannelState.OPEN) {
				stateChanged = true;
			}
		}
		
		if(isChanged()) {
			throw new StateChangeException(state);
		}
	}

	public ChannelState getState() {
		stateChanged = false;
		return state;
	}

	public boolean isChanged() {
		return stateChanged;
	}
}
