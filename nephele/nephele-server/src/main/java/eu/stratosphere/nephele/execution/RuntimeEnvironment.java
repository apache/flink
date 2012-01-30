/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.execution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The user code of every Nephele task runs inside a <code>RuntimeEnvironment</code> object. The environment provides
 * important services to the task. It keeps track of setting up the communication channels and provides access to input
 * splits, memory manager, etc.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class RuntimeEnvironment implements Environment, Runnable, IOReadableWritable {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(RuntimeEnvironment.class);

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 100;

	/**
	 * List of output gates created by the task.
	 */
	private final List<OutputGate<? extends Record>> outputGates = new CopyOnWriteArrayList<OutputGate<? extends Record>>();

	/**
	 * List of input gates created by the task.
	 */
	private final List<InputGate<? extends Record>> inputGates = new CopyOnWriteArrayList<InputGate<? extends Record>>();

	/**
	 * Queue of unbound output gate IDs which are required for deserializing an environment in the course of an RPC
	 * call.
	 */
	private final Queue<GateID> unboundOutputGateIDs = new ArrayDeque<GateID>();

	/**
	 * Queue of unbound input gate IDs which are required for deserializing an environment in the course of an RPC
	 * call.
	 */
	private final Queue<GateID> unboundInputGateIDs = new ArrayDeque<GateID>();

	/**
	 * The memory manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private volatile MemoryManager memoryManager;

	/**
	 * The io manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private volatile IOManager ioManager;

	/**
	 * Class of the task to run in this environment.
	 */
	private volatile Class<? extends AbstractInvokable> invokableClass = null;

	/**
	 * Instance of the class to be run in this environment.
	 */
	private volatile AbstractInvokable invokable = null;

	/**
	 * The thread executing the task in the environment.
	 */
	private volatile Thread executingThread = null;

	/**
	 * The ID of the job this task belongs to.
	 */
	private volatile JobID jobID = null;

	/**
	 * The task configuration encapsulated in the environment object.
	 */
	private volatile Configuration taskConfiguration = null;

	/**
	 * The job configuration encapsulated in the environment object.
	 */
	private volatile Configuration jobConfiguration = null;

	/**
	 * The input split provider that can be queried for new input splits.
	 */
	private volatile InputSplitProvider inputSplitProvider = null;

	/**
	 * The observer object for the task's execution.
	 */
	private volatile ExecutionObserver executionObserver = null;

	/**
	 * The current number of subtasks the respective task is split into.
	 */
	private volatile int currentNumberOfSubtasks = 1;

	/**
	 * The index of this subtask in the subtask group.
	 */
	private volatile int indexInSubtaskGroup = 0;

	/**
	 * The name of the task running in this environment.
	 */
	private volatile String taskName;

	/**
	 * Creates a new runtime environment object which contains the runtime information for the encapsulated Nephele
	 * task.
	 * 
	 * @param jobID
	 *        the ID of the original Nephele job
	 * @param taskName
	 *        the name of task running in this environment
	 * @param invokableClass
	 *        invokableClass the class that should be instantiated as a Nephele task
	 * @param taskConfiguration
	 *        the configuration object which was attached to the original {@link JobVertex}
	 * @param jobConfiguration
	 *        the configuration object which was attached to the original {@link JobGraph}
	 */
	public RuntimeEnvironment(final JobID jobID, final String taskName,
			final Class<? extends AbstractInvokable> invokableClass, final Configuration taskConfiguration,
			final Configuration jobConfiguration) {
		this.jobID = jobID;
		this.taskName = taskName;
		this.invokableClass = invokableClass;
		this.taskConfiguration = taskConfiguration;
		this.jobConfiguration = jobConfiguration;
	}

	/**
	 * Empty constructor used to deserialize the object.
	 */
	public RuntimeEnvironment() {
	}

	/**
	 * Returns the invokable object that represents the Nephele task.
	 * 
	 * @return the invokable object that represents the Nephele task
	 */
	public AbstractInvokable getInvokable() {
		return this.invokable;
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
	public GateID getNextUnboundInputGateID() {

		return this.unboundInputGateIDs.poll();
	}

	/**
	 * {@inheritDoc}
	 */
	public GateID getNextUnboundOutputGateID() {

		return this.unboundOutputGateIDs.poll();
	}

	/**
	 * Creates a new instance of the Nephele task and registers it with its
	 * environment.
	 * 
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public void instantiateInvokable() throws Exception {

		// Test and set, protected by synchronized block
		synchronized (this) {

			if (this.invokableClass == null) {
				LOG.fatal("InvokableClass is null");
				return;
			}

			try {
				this.invokable = this.invokableClass.newInstance();
			} catch (InstantiationException e) {
				LOG.error(e);
			} catch (IllegalAccessException e) {
				LOG.error(e);
			}
		}

		this.invokable.setEnvironment(this);
		this.invokable.registerInputOutput();

		if (this.jobID == null) {
			LOG.warn("jobVertexID is null");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		if (invokable == null) {
			LOG.fatal("ExecutionEnvironment has no Invokable set");
		}

		// Now the actual program starts to run
		changeExecutionState(ExecutionState.RUNNING, null);

		// If the task has been canceled in the mean time, do not even start it
		if (this.executionObserver.isCanceled()) {
			changeExecutionState(ExecutionState.CANCELED, null);
			return;
		}

		try {

			// Activate input channels
			activateInputChannels();

			this.invokable.invoke();

			// Make sure, we enter the catch block when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

		} catch (Exception e) {

			if (!this.executionObserver.isCanceled()) {

				// Perform clean up when the task failed and has been not canceled by the user
				try {
					this.invokable.cancel();
				} catch (Exception e2) {
					LOG.error(StringUtils.stringifyException(e2));
				}
			}

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Task finished running, but there may be unconsumed output data in some of the channels
		changeExecutionState(ExecutionState.FINISHING, null);

		try {
			// If there is any unclosed input gate, close it and propagate close operation to corresponding output gate
			closeInputGates();

			// First, close all output gates to indicate no records will be emitted anymore
			requestAllOutputGatesToClose();

			// Wait until all input channels are closed
			waitForInputChannelsToBeClosed();

			// Now we wait until all output channels have written out their data and are closed
			waitForOutputChannelsToBeClosed();
		} catch (Exception e) {

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Release all resources that may currently be allocated by the individual channels
		releaseAllChannelResources();

		// Finally, switch execution state to FINISHED and report to job manager
		changeExecutionState(ExecutionState.FINISHED, null);
	}

	/**
	 * Activates all of the task's input channels.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while transmitting one of the activation requests to the corresponding
	 *         output channels
	 * @throws InterruptedException
	 *         throws if the task is interrupted while waiting for the activation process to complete
	 */
	private void activateInputChannels() throws IOException, InterruptedException {

		for (int i = 0; i < getNumberOfInputGates(); ++i) {
			this.inputGates.get(i).activateInputChannels();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGate<? extends Record> createOutputGate(final GateID gateID, Class<? extends Record> outputClass,
			final ChannelSelector<? extends Record> selector, final boolean isBroadcast) {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		final RuntimeOutputGate<? extends Record> rog = (RuntimeOutputGate<? extends Record>) new RuntimeOutputGate(
			getJobID(), gateID, outputClass, getNumberOfOutputGates(), selector, isBroadcast);

		return rog;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGate<? extends Record> createInputGate(final GateID gateID,
			final RecordDeserializer<? extends Record> deserializer, final DistributionPattern distributionPattern) {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		final RuntimeInputGate<? extends Record> rig = (RuntimeInputGate<? extends Record>) new RuntimeInputGate(
			getJobID(), gateID, deserializer, getNumberOfInputGates(), distributionPattern);

		return rig;
	}

	@Override
	public void registerOutputGate(OutputGate<? extends Record> outputGate) {

		this.outputGates.add(outputGate);
	}

	@Override
	public void registerInputGate(InputGate<? extends Record> inputGate) {

		this.inputGates.add(inputGate);
	}

	/**
	 * {@inheritDoc}
	 */
	public int getNumberOfOutputGates() {
		return this.outputGates.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {
		return this.inputGates.size();
	}

	/**
	 * Returns the registered input gate with index <code>pos</code>.
	 * 
	 * @param pos
	 *        the index of the input gate to return
	 * @return the input gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public InputGate<? extends Record> getInputGate(final int pos) {
		if (pos < this.inputGates.size()) {
			return this.inputGates.get(pos);
		}

		return null;
	}

	/**
	 * Returns the registered output gate with index <code>pos</code>.
	 * 
	 * @param pos
	 *        the index of the output gate to return
	 * @return the output gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public OutputGate<? extends Record> getOutputGate(final int pos) {
		if (pos < this.outputGates.size()) {
			return this.outputGates.get(pos);
		}

		return null;
	}

	/**
	 * Returns the thread which is assigned to execute the user code.
	 * 
	 * @return the thread which is assigned to execute the user code
	 */
	public Thread getExecutingThread() {

		synchronized (this) {

			if (this.executingThread == null) {
				if (this.taskName == null) {
					this.executingThread = new Thread(this);
				} else {
					this.executingThread = new Thread(this, this.taskName);
				}
			}

			return this.executingThread;
		}
	}

	// TODO: See if type safety can be improved here
	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void read(final DataInput in) throws IOException {

		// Read job vertex id
		this.jobID = new JobID();
		this.jobID.read(in);

		// Read the task name
		this.taskName = StringRecord.readString(in);

		// Read names of required jar files
		final String[] requiredJarFiles = new String[in.readInt()];
		for (int i = 0; i < requiredJarFiles.length; i++) {
			requiredJarFiles[i] = StringRecord.readString(in);
		}

		// Now register data with the library manager
		LibraryCacheManager.register(this.jobID, requiredJarFiles);

		// Get ClassLoader from Library Manager
		final ClassLoader cl = LibraryCacheManager.getClassLoader(this.jobID);

		// Read the name of the invokable class;
		final String invokableClassName = StringRecord.readString(in);

		if (invokableClassName == null) {
			throw new IOException("invokableClassName is null");
		}

		try {
			this.invokableClass = (Class<? extends AbstractInvokable>) Class.forName(invokableClassName, true, cl);
		} catch (ClassNotFoundException cnfe) {
			throw new IOException("Class " + invokableClassName + " not found in one of the supplied jar files: "
				+ StringUtils.stringifyException(cnfe));
		}

		final int numOuputGates = in.readInt();

		for (int i = 0; i < numOuputGates; i++) {

			final GateID gateID = new GateID();
			gateID.read(in);
			this.unboundOutputGateIDs.add(gateID);
		}

		final int numInputGates = in.readInt();

		for (int i = 0; i < numInputGates; i++) {

			final GateID gateID = new GateID();
			gateID.read(in);
			this.unboundInputGateIDs.add(gateID);
		}

		// The configuration objects
		this.taskConfiguration = new Configuration();
		this.taskConfiguration.read(in);
		this.jobConfiguration = new Configuration();
		this.jobConfiguration.read(in);

		// The current of number subtasks
		this.currentNumberOfSubtasks = in.readInt();
		// The index in the subtask group
		this.indexInSubtaskGroup = in.readInt();

		// Finally, instantiate the invokable object
		try {
			instantiateInvokable();
		} catch (Exception e) {
			throw new IOException(StringUtils.stringifyException(e));
		}

		// Output channels
		for (int i = 0; i < numOuputGates; ++i) {
			final OutputGate<? extends Record> outputGate = this.outputGates.get(i);
			final int numberOfOutputChannels = in.readInt();
			for (int j = 0; j < numberOfOutputChannels; ++j) {
				final ChannelID channelID = new ChannelID();
				channelID.read(in);
				final ChannelID connectedChannelID = new ChannelID();
				connectedChannelID.read(in);
				final ChannelType channelType = EnumUtils.readEnum(in, ChannelType.class);
				final CompressionLevel compressionLevel = EnumUtils.readEnum(in, CompressionLevel.class);

				AbstractOutputChannel<? extends Record> outputChannel = null;

				switch (channelType) {
				case INMEMORY:
					outputChannel = outputGate.createInMemoryOutputChannel((OutputGate) outputGate, channelID,
						compressionLevel);
					break;
				case NETWORK:
					outputChannel = outputGate.createNetworkOutputChannel((OutputGate) outputGate, channelID,
						compressionLevel);
					break;
				case FILE:
					outputChannel = outputGate.createFileOutputChannel((OutputGate) outputGate, channelID,
						compressionLevel);
					break;
				}

				if (outputChannel == null) {
					throw new IOException("Unable to create output channel for channel ID " + channelID);
				}

				outputChannel.setConnectedChannelID(connectedChannelID);
			}
		}

		// Input channels
		for (int i = 0; i < numInputGates; ++i) {
			final InputGate<? extends Record> inputGate = this.inputGates.get(i);
			final int numberOfInputChannels = in.readInt();
			for (int j = 0; j < numberOfInputChannels; ++j) {
				final ChannelID channelID = new ChannelID();
				channelID.read(in);
				final ChannelID connectedChannelID = new ChannelID();
				connectedChannelID.read(in);
				final ChannelType channelType = EnumUtils.readEnum(in, ChannelType.class);
				final CompressionLevel compressionLevel = EnumUtils.readEnum(in, CompressionLevel.class);

				AbstractInputChannel<? extends Record> inputChannel = null;

				switch (channelType) {
				case INMEMORY:
					inputChannel = inputGate.createInMemoryInputChannel((InputGate) inputGate, channelID,
						compressionLevel);
					break;
				case NETWORK:
					inputChannel = inputGate.createNetworkInputChannel((InputGate) inputGate, channelID,
						compressionLevel);
					break;
				case FILE:
					inputChannel = inputGate.createFileInputChannel((InputGate) inputGate, channelID, compressionLevel);
					break;
				}

				if (inputChannel == null) {
					throw new IOException("Unable to create output channel for channel ID " + channelID);
				}

				inputChannel.setConnectedChannelID(connectedChannelID);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		// Write out job vertex id
		if (this.jobID == null) {
			throw new IOException("this.jobID is null");
		}

		this.jobID.write(out);

		// Write the task name
		StringRecord.writeString(out, this.taskName);

		// Write out the names of the required jar files
		final String[] requiredJarFiles = LibraryCacheManager.getRequiredJarFiles(this.jobID);

		out.writeInt(requiredJarFiles.length);
		for (int i = 0; i < requiredJarFiles.length; i++) {
			StringRecord.writeString(out, requiredJarFiles[i]);
		}

		// Write out the name of the invokable class
		if (this.invokableClass == null) {
			throw new IOException("this.invokableClass is null");
		}

		StringRecord.writeString(out, this.invokableClass.getName());

		// Output gates
		final int numberOfOutputGates = this.outputGates.size();
		out.writeInt(numberOfOutputGates);
		for (int i = 0; i < numberOfOutputGates; ++i) {
			final OutputGate<? extends Record> outputGate = this.outputGates.get(i);
			outputGate.getGateID().write(out);
		}

		// Input gates
		final int numberOfInputGates = this.inputGates.size();
		out.writeInt(numberOfInputGates);
		for (int i = 0; i < numberOfInputGates; i++) {
			final InputGate<? extends Record> inputGate = this.inputGates.get(i);
			inputGate.getGateID().write(out);
		}

		// The configuration objects
		this.taskConfiguration.write(out);
		this.jobConfiguration.write(out);

		// The current of number subtasks
		out.writeInt(this.currentNumberOfSubtasks);
		// The index in the subtask group
		out.writeInt(this.indexInSubtaskGroup);

		// Output channels
		for (int i = 0; i < numberOfOutputGates; ++i) {
			final OutputGate<? extends Record> outputGate = this.outputGates.get(i);
			final int numberOfOutputChannels = outputGate.getNumberOfOutputChannels();
			out.writeInt(numberOfOutputChannels);
			for (int j = 0; j < numberOfOutputChannels; ++j) {
				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
				outputChannel.getID().write(out);
				outputChannel.getConnectedChannelID().write(out);
				EnumUtils.writeEnum(out, outputChannel.getType());
				EnumUtils.writeEnum(out, outputChannel.getCompressionLevel());
			}
		}

		// Input channels
		for (int i = 0; i < numberOfInputGates; ++i) {
			final InputGate<? extends Record> inputGate = this.inputGates.get(i);
			final int numberOfInputChannels = inputGate.getNumberOfInputChannels();
			out.writeInt(numberOfInputChannels);
			for (int j = 0; j < numberOfInputChannels; ++j) {
				final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(j);
				inputChannel.getID().write(out);
				inputChannel.getConnectedChannelID().write(out);
				EnumUtils.writeEnum(out, inputChannel.getType());
				EnumUtils.writeEnum(out, inputChannel.getCompressionLevel());
			}
		}
	}

	/**
	 * Blocks until all output channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the output channels
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForOutputChannelsToBeClosed() throws IOException, InterruptedException {

		// Wait for disconnection of all output gates
		while (true) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfOutputGates(); i++) {
				final OutputGate<? extends Record> og = this.outputGates.get(i);
				if (!og.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				Thread.sleep(SLEEPINTERVAL);
			}
		}
	}

	/**
	 * Blocks until all input channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the input channels
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForInputChannelsToBeClosed() throws IOException, InterruptedException {

		// Wait for disconnection of all output gates
		while (true) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfInputGates(); i++) {
				final InputGate<? extends Record> eig = this.inputGates.get(i);
				if (!eig.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				Thread.sleep(SLEEPINTERVAL);
			}
		}
	}

	/**
	 * Closes all input gates which are not already closed.
	 */
	private void closeInputGates() throws IOException, InterruptedException {

		for (int i = 0; i < this.inputGates.size(); i++) {
			final InputGate<? extends Record> eig = this.inputGates.get(i);
			// Important: close must be called on each input gate exactly once
			eig.close();
		}

	}

	/**
	 * Requests all output gates to be closed.
	 */
	private void requestAllOutputGatesToClose() throws IOException, InterruptedException {

		for (int i = 0; i < this.outputGates.size(); i++) {
			this.outputGates.get(i).requestClose();
		}
	}

	/**
	 * Returns a duplicate (deep copy) of this environment object. However, duplication
	 * does not cover the gates arrays. They must be manually reconstructed.
	 * 
	 * @return a duplicate (deep copy) of this environment object
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public RuntimeEnvironment duplicateEnvironment() throws Exception {

		final RuntimeEnvironment duplicatedEnvironment = new RuntimeEnvironment();
		duplicatedEnvironment.invokableClass = this.invokableClass;
		duplicatedEnvironment.jobID = this.jobID;
		duplicatedEnvironment.taskName = this.taskName;
		Thread tmpThread = null;
		synchronized (this) {
			tmpThread = this.executingThread;
		}
		synchronized (duplicatedEnvironment) {
			duplicatedEnvironment.executingThread = tmpThread;
		}
		duplicatedEnvironment.taskConfiguration = this.taskConfiguration;
		duplicatedEnvironment.jobConfiguration = this.jobConfiguration;

		// We instantiate the invokable of the new environment
		duplicatedEnvironment.instantiateInvokable();

		return duplicatedEnvironment;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {
		return this.ioManager;
	}

	/**
	 * Sets the {@link IOManager}.
	 * 
	 * @param memoryManager
	 *        the new {@link IOManager}
	 */
	public void setIOManager(final IOManager ioManager) {
		this.ioManager = ioManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	/**
	 * Sets the {@link MemoryManager}.
	 * 
	 * @param memoryManager
	 *        the new {@link MemoryManager}
	 */
	public void setMemoryManager(final MemoryManager memoryManager) {
		this.memoryManager = memoryManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCurrentNumberOfSubtasks() {

		return this.currentNumberOfSubtasks;
	}

	/**
	 * Sets the current number of subtasks the respective task is split into.
	 * 
	 * @param currentNumberOfSubtasks
	 *        the current number of subtasks the respective task is split into
	 */
	public void setCurrentNumberOfSubtasks(final int currentNumberOfSubtasks) {

		this.currentNumberOfSubtasks = currentNumberOfSubtasks;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndexInSubtaskGroup() {

		return this.indexInSubtaskGroup;
	}

	/**
	 * Sets the index of this subtask in the subtask group.
	 * 
	 * @param indexInSubtaskGroup
	 *        the index of this subtask in the subtask group
	 */
	public void setIndexInSubtaskGroup(final int indexInSubtaskGroup) {

		this.indexInSubtaskGroup = indexInSubtaskGroup;
	}

	private void changeExecutionState(final ExecutionState newExecutionState, final String optionalMessage) {

		if (this.executionObserver != null) {
			this.executionObserver.executionStateChanged(newExecutionState, optionalMessage);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskName() {

		return this.taskName;
	}

	/**
	 * Sets the execution observer for this environment.
	 * 
	 * @param executionObserver
	 *        the execution observer for this environment
	 */
	public void setExecutionObserver(final ExecutionObserver executionObserver) {
		this.executionObserver = executionObserver;
	}

	/**
	 * Sets the input split provider for this environment.
	 * 
	 * @param inputSplitProvider
	 *        the input split provider for this environment
	 */
	public void setInputSplitProvider(final InputSplitProvider inputSplitProvider) {
		this.inputSplitProvider = inputSplitProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		if (this.executionObserver != null) {
			this.executionObserver.userThreadStarted(userThread);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Thread userThread) {

		if (this.executionObserver != null) {
			this.executionObserver.userThreadFinished(userThread);
		}
	}

	/**
	 * Releases the allocated resources (particularly buffer) of input and output channels attached to this task. This
	 * method should only be called after the respected task has stopped running.
	 */
	private void releaseAllChannelResources() {

		for (int i = 0; i < this.inputGates.size(); i++) {
			this.inputGates.get(i).releaseAllChannelResources();
		}

		for (int i = 0; i < this.outputGates.size(); i++) {
			this.outputGates.get(i).releaseAllChannelResources();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDs() {

		final Set<ChannelID> outputChannelIDs = new HashSet<ChannelID>();

		final Iterator<OutputGate<? extends Record>> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {

			final OutputGate<? extends Record> outputGate = gateIterator.next();
			for (int i = 0; i < outputGate.getNumberOfOutputChannels(); ++i) {
				outputChannelIDs.add(outputGate.getOutputChannel(i).getID());
			}
		}

		return Collections.unmodifiableSet(outputChannelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDs() {

		final Set<ChannelID> inputChannelIDs = new HashSet<ChannelID>();

		final Iterator<InputGate<? extends Record>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {

			final InputGate<? extends Record> outputGate = gateIterator.next();
			for (int i = 0; i < outputGate.getNumberOfInputChannels(); ++i) {
				inputChannelIDs.add(outputGate.getInputChannel(i).getID());
			}
		}

		return Collections.unmodifiableSet(inputChannelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getInputGateIDs() {

		final Set<GateID> inputGateIDs = new HashSet<GateID>();

		final Iterator<InputGate<? extends Record>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {
			inputGateIDs.add(gateIterator.next().getGateID());
		}

		return Collections.unmodifiableSet(inputGateIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getOutputGateIDs() {

		final Set<GateID> outputGateIDs = new HashSet<GateID>();

		final Iterator<OutputGate<? extends Record>> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {
			outputGateIDs.add(gateIterator.next().getGateID());
		}

		return Collections.unmodifiableSet(outputGateIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {

		OutputGate<? extends Record> outputGate = null;
		final Iterator<OutputGate<? extends Record>> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {
			final OutputGate<? extends Record> candidateGate = gateIterator.next();
			if (candidateGate.getGateID().equals(gateID)) {
				outputGate = candidateGate;
				break;
			}
		}

		if (outputGate == null) {
			throw new IllegalArgumentException("Cannot find output gate with ID " + gateID);
		}

		final Set<ChannelID> outputChannelIDs = new HashSet<ChannelID>();

		for (int i = 0; i < outputGate.getNumberOfOutputChannels(); ++i) {
			outputChannelIDs.add(outputGate.getOutputChannel(i).getID());
		}

		return Collections.unmodifiableSet(outputChannelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {

		InputGate<? extends Record> inputGate = null;
		final Iterator<InputGate<? extends Record>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {
			final InputGate<? extends Record> candidateGate = gateIterator.next();
			if (candidateGate.getGateID().equals(gateID)) {
				inputGate = candidateGate;
				break;
			}
		}

		if (inputGate == null) {
			throw new IllegalArgumentException("Cannot find input gate with ID " + gateID);
		}

		final Set<ChannelID> inputChannelIDs = new HashSet<ChannelID>();

		for (int i = 0; i < inputGate.getNumberOfInputChannels(); ++i) {
			inputChannelIDs.add(inputGate.getInputChannel(i).getID());
		}

		return Collections.unmodifiableSet(inputChannelIDs);
	}
}
