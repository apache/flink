package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;

final class ReplayTaskContext implements TaskContext {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGateContext createOutputGateContext(final GateID gateID) {

		return new ReplayOutputGateContext(gateID);
	}

	@Override
	public InputGateContext createInputGateContext(final GateID gateID) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LocalBufferPoolOwner getLocalBufferPoolOwner() {

		return null;
	}

}
