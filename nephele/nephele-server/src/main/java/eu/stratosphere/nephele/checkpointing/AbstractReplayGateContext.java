package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.GateContext;

abstract class AbstractReplayGateContext implements GateContext {

	private final GateID gateID;

	AbstractReplayGateContext(final GateID gateID) {
		this.gateID = gateID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getGateID() {

		return this.gateID;
	}

}
