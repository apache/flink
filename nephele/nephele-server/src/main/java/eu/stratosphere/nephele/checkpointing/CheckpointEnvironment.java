package eu.stratosphere.nephele.checkpointing;

import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
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

	private final ExecutionVertexID vertexID;

	private final Environment environment;

	private final boolean hasCompleteCheckpoint;

	private final Map<ChannelID, ReplayOutputBroker> outputBrokerMap;

	/**
	 * The observer object for the task's execution.
	 */
	private volatile ExecutionObserver executionObserver = null;

	private volatile ReplayThread executingThread = null;

	CheckpointEnvironment(final ExecutionVertexID vertexID, final Environment environment,
			final boolean hasCompleteCheckpoint,
			final Map<ChannelID, ReplayOutputBroker> outputBrokerMap) {

		this.vertexID = vertexID;
		this.environment = environment;
		this.hasCompleteCheckpoint = hasCompleteCheckpoint;
		this.outputBrokerMap = outputBrokerMap;
	}

	/**
	 * Sets the execution observer for this environment.
	 * 
	 * @param executionObserver
	 *        the execution observer for this environment
	 */
	void setExecutionObserver(final ExecutionObserver executionObserver) {
		this.executionObserver = executionObserver;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.environment.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getTaskConfiguration() {

		return this.environment.getTaskConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getJobConfiguration() {

		return this.environment.getJobConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCurrentNumberOfSubtasks() {

		return this.environment.getCurrentNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndexInSubtaskGroup() {

		return this.environment.getIndexInSubtaskGroup();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		throw new IllegalStateException("Checkpoint replay task called userThreadStarted");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Thread userThread) {

		throw new IllegalStateException("Checkpoint replay task called userThreadFinished");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {

		throw new IllegalStateException("Checkpoint replay task called getInputSplitProvider");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {

		throw new IllegalStateException("Checkpoint replay task called getIOManager");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {

		throw new IllegalStateException("Checkpoint replay task called getMemoryManager");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskName() {

		return this.environment.getTaskName();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getNextUnboundInputGateID() {

		throw new IllegalStateException("Checkpoint replay task called getNextUnboundInputGateID");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getNextUnboundOutputGateID() {

		throw new IllegalStateException("Checkpoint replay task called getNextUnboundOutputGateID");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputGates() {

		return this.environment.getNumberOfOutputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {

		return this.environment.getNumberOfInputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGate<? extends Record> createOutputGate(final GateID gateID,
			final Class<? extends Record> outputClass,
			final ChannelSelector<? extends Record> selector, final boolean isBroadcast) {

		throw new IllegalStateException("Checkpoint replay task called createOutputGate");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGate<? extends Record> createInputGate(final GateID gateID,
			final RecordDeserializer<? extends Record> deserializer, final DistributionPattern distributionPattern) {

		throw new IllegalStateException("Checkpoint replay task called createInputGate");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerOutputGate(final OutputGate<? extends Record> outputGate) {

		throw new IllegalStateException("Checkpoint replay task called registerOutputGate");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputGate(final InputGate<? extends Record> inputGate) {

		throw new IllegalStateException("Checkpoint replay task called registerInputGate");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDs() {

		return this.environment.getOutputChannelIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDs() {

		return this.environment.getInputChannelIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getOutputGateIDs() {

		return this.environment.getOutputGateIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getInputGateIDs() {

		return this.environment.getInputGateIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {

		return this.environment.getOutputChannelIDsOfGate(gateID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {

		return this.environment.getInputChannelIDsOfGate(gateID);
	}

	/**
	 * Returns the thread which is assigned to executes the replay task
	 * 
	 * @return the thread which is assigned to execute the replay task
	 */
	public ReplayThread getExecutingThread() {

		synchronized (this) {

			if (this.executingThread == null) {
				this.executingThread = new ReplayThread(this.vertexID, this.executionObserver, getTaskName(),
					this.hasCompleteCheckpoint, this.outputBrokerMap);
			}

			return this.executingThread;
		}
	}

	// DW: Start of temporary code
	@Override
	public void reportPACTDataStatistics(final long numberOfConsumedBytes, final long numberOfProducedBytes) {
		
		throw new IllegalStateException("reportPACTDataStatistics called on CheckpointEnvironment");
	}
	// DW: End of temporary code
}
