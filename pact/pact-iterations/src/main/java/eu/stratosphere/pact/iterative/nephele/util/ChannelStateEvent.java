package eu.stratosphere.pact.iterative.nephele.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;

public class ChannelStateEvent extends AbstractTaskEvent {
	public static enum ChannelState {
		STARTED((byte) 1), OPEN((byte) 2), CLOSED((byte) 3), TERMINATED((byte) 4);
		
		final byte id;
		
		ChannelState(byte id) {
			this.id = id;
		}
		
		public byte getStateId() {
			return id;
		}
		
		public static ChannelState getStateById(byte id) {
			for (ChannelState state : ChannelState.values()) {
				if(state.getStateId() == id) {
					return state;
				}
			}
			
			throw new IllegalArgumentException("This id " + id + " has no corresponding channel state");
		}
	}

	private ChannelState state;
	
	public ChannelStateEvent() {
		
	}
	
	public ChannelStateEvent(ChannelState state) {
		this.state = state;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.write(state.getStateId());
	}

	@Override
	public void read(DataInput in) throws IOException {
		state = ChannelState.getStateById(in.readByte());
	}

	public ChannelState getState() {
		return state;
	}

}
