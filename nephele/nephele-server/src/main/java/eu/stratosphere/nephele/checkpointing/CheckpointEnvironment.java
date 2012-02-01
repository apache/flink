package eu.stratosphere.nephele.checkpointing;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;

final class CheckpointEnvironment implements Environment {

	private final JobID jobID;

	private final Map<GateID, Set<ChannelID>> outputChannelIDs;

	private final Map<GateID, Set<ChannelID>> inputChannelIDs;

	private CheckpointEnvironment(final JobID jobID, Map<GateID, Set<ChannelID>> outputChannelIDs,
			Map<GateID, Set<ChannelID>> inputChannelIDs) {

		this.jobID = jobID;
		this.outputChannelIDs = outputChannelIDs;
		this.inputChannelIDs = inputChannelIDs;
	}

	static CheckpointEnvironment createFromEnvironment(final Environment environment) {

		final JobID jobID = environment.getJobID();

		final Map<GateID, Set<ChannelID>> outputChannelIDs = new HashMap<GateID, Set<ChannelID>>();

		final Map<GateID, Set<ChannelID>> inputChannelIDs = new HashMap<GateID, Set<ChannelID>>();

		Iterator<GateID> gateIt = environment.getOutputGateIDs().iterator();
		while (gateIt.hasNext()) {

			final GateID gateID = gateIt.next();
			outputChannelIDs.put(gateID, environment.getOutputChannelIDsOfGate(gateID));
		}

		gateIt = environment.getInputGateIDs().iterator();
		while (gateIt.hasNext()) {

			final GateID gateID = gateIt.next();
			inputChannelIDs.put(gateID, environment.getInputChannelIDsOfGate(gateID));
		}

		return new CheckpointEnvironment(jobID, Collections.unmodifiableMap(outputChannelIDs),
			Collections.unmodifiableMap(inputChannelIDs));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.jobID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getTaskConfiguration() {

		throw new UnsupportedOperationException("Method getTaskConfiguration is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getJobConfiguration() {

		throw new UnsupportedOperationException("Method getJobConfiguration is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCurrentNumberOfSubtasks() {

		throw new UnsupportedOperationException(
			"Method getCurrentNumberOfSubtasks is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndexInSubtaskGroup() {

		throw new UnsupportedOperationException("Method getIndexInSubtaskGroup is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		throw new UnsupportedOperationException("Method userThreadStarted is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Thread userThread) {

		throw new UnsupportedOperationException("Method userThreadFinished is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {

		throw new UnsupportedOperationException("Method getInputSplitProvider is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {

		throw new UnsupportedOperationException("Method getIOManager is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {

		throw new UnsupportedOperationException("Method getMemoryManager is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskName() {

		throw new UnsupportedOperationException("Method getTaskName is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getNextUnboundInputGateID() {

		throw new UnsupportedOperationException("Method getNextUnboundInputGateID is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getNextUnboundOutputGateID() {

		throw new UnsupportedOperationException(
			"Method getNextUnboundOutputGateID is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputGates() {

		throw new UnsupportedOperationException("Method getNumberOfOutputGates is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {

		throw new UnsupportedOperationException("Method getNumberOfInputGates is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGate<? extends Record> createOutputGate(final GateID gateID, Class<? extends Record> outputClass,
			final ChannelSelector<? extends Record> selector, final boolean isBroadcast) {

		throw new UnsupportedOperationException("Method createOutputGate is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGate<? extends Record> createInputGate(final GateID gateID,
			final RecordDeserializer<? extends Record> deserializer, final DistributionPattern distributionPattern) {

		throw new UnsupportedOperationException("Method createInputGate is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerOutputGate(final OutputGate<? extends Record> outputGate) {

		throw new UnsupportedOperationException("Method registerOutputGate is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputGate(final InputGate<? extends Record> inputGate) {

		throw new UnsupportedOperationException("Method registerInputGate is not supported by this environment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDs() {

		final Set<ChannelID> channelIDs = new HashSet<ChannelID>();
		final Iterator<Map.Entry<GateID, Set<ChannelID>>> it = this.outputChannelIDs.entrySet().iterator();
		while (it.hasNext()) {

			final Map.Entry<GateID, Set<ChannelID>> entry = it.next();
			channelIDs.addAll(entry.getValue());

		}

		return Collections.unmodifiableSet(channelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDs() {

		final Set<ChannelID> channelIDs = new HashSet<ChannelID>();
		final Iterator<Map.Entry<GateID, Set<ChannelID>>> it = this.inputChannelIDs.entrySet().iterator();
		while (it.hasNext()) {

			final Map.Entry<GateID, Set<ChannelID>> entry = it.next();
			channelIDs.addAll(entry.getValue());

		}

		return Collections.unmodifiableSet(channelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getOutputGateIDs() {

		return this.outputChannelIDs.keySet();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getInputGateIDs() {

		return this.inputChannelIDs.keySet();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {

		final Set<ChannelID> channelIDs = this.outputChannelIDs.get(gateID);
		if (channelIDs == null) {
			throw new IllegalStateException("Cannot find channel IDs for output gate with ID " + gateID);
		}

		return channelIDs;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {

		final Set<ChannelID> channelIDs = this.inputChannelIDs.get(gateID);
		if (channelIDs == null) {
			throw new IllegalStateException("Cannot find channel IDs for input gate with ID " + gateID);
		}

		return channelIDs;
	}

}
