package eu.stratosphere.nephele.taskmanager.runtime;

import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.types.Record;

final class EnvelopeConsumptionTracker {

	private final EnvelopeConsumptionLog log;

	private final RuntimeEnvironment environment;

	EnvelopeConsumptionTracker(final ExecutionVertexID vertexID, final RuntimeEnvironment environment) {

		this.log = new EnvelopeConsumptionLog(vertexID, this);
		this.environment = environment;
	}

	public synchronized void reportEnvelopeAvailability(
			final AbstractByteBufferedInputChannel<? extends Record> inputChannel) {

		this.log.add(inputChannel.getInputGate().getIndex(), inputChannel.getChannelIndex());
	}
	
	public boolean followsLog() {
		
		return this.log.followsLog();
	}

	public synchronized void finishLog() {
		this.log.finish();
	}

	public void reportEnvelopeConsumed(
			final AbstractByteBufferedInputChannel<? extends Record> inputChannel) {

		inputChannel.notifyDataUnitConsumed();
	}

	void announceData(final int gateIndex, final int channelIndex) {

		final InputGate<? extends Record> inputGate = this.environment.getInputGate(gateIndex);

		final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(channelIndex);

		((AbstractByteBufferedInputChannel<? extends Record>) inputChannel).checkForNetworkEvents();
	}
}
