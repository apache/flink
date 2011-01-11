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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * In Nephele every task runs inside an <code>Environment</code> object. At the beginning of a task
 * execution the corresponding environment object is sent the responsible task manager and is registered
 * there. The <code>Environment</code> object takes care of the correct initialization of a task and spans
 * the thread that executes the actual task program.
 * 
 * @author warneke
 */
public class Environment implements Runnable, IOReadableWritable {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(Environment.class);

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 500;

	/**
	 * List of output gates created by the task.
	 */
	private final List<OutputGate<? extends Record>> outputGates = new ArrayList<OutputGate<? extends Record>>();

	/**
	 * List of input gates created by the task.
	 */
	private final List<InputGate<? extends Record>> inputGates = new ArrayList<InputGate<? extends Record>>();

	/**
	 * List of output gates which have to be rebound to a task after transferring the environment to a TaskManager.
	 */
	private final List<OutputGate<? extends Record>> unboundOutputGates = new ArrayList<OutputGate<? extends Record>>();

	/**
	 * List of input gates which have to be rebound to a task after transferring the environment to a TaskManager.
	 */
	private final List<InputGate<? extends Record>> unboundInputGates = new ArrayList<InputGate<? extends Record>>();

	/**
	 * The memory manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private MemoryManager memoryManager;

	/**
	 * The io manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private IOManager ioManager;

	/**
	 * Class of the task to run in this environment.
	 */
	private Class<? extends AbstractInvokable> invokableClass = null;

	/**
	 * Instance of the class to be run in this environment.
	 */
	private AbstractInvokable invokable = null;

	/**
	 * The thread executing the task in the environment.
	 */
	private Thread executingThread = null;

	/**
	 * List of input splits assigned to this environment.
	 */
	private final ArrayList<InputSplit> inputSplits = new ArrayList<InputSplit>();

	/**
	 * Current execution state of the task associated with this environment.
	 */
	private ExecutionState executionState = ExecutionState.CREATED;

	/**
	 * The ID of the job this task belongs to.
	 */
	private JobID jobID = null;

	/**
	 * List of {@link ExecutionListener} object which shall be notified about changes of the execution state.
	 */
	private final List<ExecutionListener> executionListeners = new ArrayList<ExecutionListener>();

	/**
	 * The runtime configuration of the task encapsulated in the environment object.
	 */
	private Configuration runtimeConfiguration = null;

	/**
	 * The current number of subtasks the respective task is split into.
	 */
	private int currentNumberOfSubtasks = 1;

	/**
	 * The index of this subtask in the subtask group.
	 */
	private int indexInSubtaskGroup = 0;

	/**
	 * The name of the task running in this environment.
	 */
	private String taskName;

	/**
	 * Creates a new environment object which contains the runtime information for the encapsulated Nephele task.
	 * 
	 * @param jobID
	 *        the ID of the original Nephele job
	 * @param taskName
	 *        the name of task running in this environment
	 * @param invokableClass
	 *        invokableClass the class that should be instantiated as a Nephele task
	 * @param runtimeConfiguration
	 *        the configuration object which was attached to the original {@link JobVertex}
	 */
	public Environment(JobID jobID, String taskName, Class<? extends AbstractInvokable> invokableClass,
			Configuration runtimeConfiguration) {
		this.jobID = jobID;
		this.taskName = taskName;
		this.invokableClass = invokableClass;
		this.runtimeConfiguration = runtimeConfiguration;
	}

	/**
	 * Empty constructor used to deserialize the object.
	 */
	public Environment() {
	}

	/**
	 * Returns the execution state of the task that runs inside this environment.
	 * 
	 * @return the execution state of the task that runs inside this environment.
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
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
	 * Returns the ID of the job from the original job graph. It is used by the library cache manager to find the
	 * required
	 * libraries for executing the assigned Nephele task.
	 * 
	 * @return the ID of the job from the original job graph
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Registers the {@link ExecutionListener} object for this environment. This object
	 * will be notified about important events during the task execution.
	 * 
	 * @param executionListener
	 *        the object to be notified for important events during the task execution
	 */
	public void registerExecutionListener(ExecutionListener executionListener) {

		synchronized (this.executionListeners) {

			if (!this.executionListeners.contains(executionListener)) {
				this.executionListeners.add(executionListener);
			}
		}
	}

	/**
	 * Unregisters the {@link ExecutionListener} object for this environment. This object
	 * will no longer be notified about important events during the task execution.
	 * 
	 * @param executionListener the lister object to be unregistered
	 */
	public void unregisterExecutionListener(ExecutionListener executionListener) {

		synchronized (this.executionListeners) {
			this.executionListeners.remove(executionListener);
		}
	}

	/**
	 * Checks if the environment has unbound input gates.
	 * 
	 * @return <code>true</code> if the environment has unbound input gates, <code>false</code> otherwise
	 */
	public boolean hasUnboundInputGates() {

		return (this.unboundInputGates.size() > 0);
	}

	/**
	 * Checks if the environment has unbound output gates.
	 * 
	 * @return <code>true</code> if the environment has unbound output gates, <code>false</code> otherwise
	 */
	public boolean hasUnboundOutputGates() {

		return (this.unboundOutputGates.size() > 0);
	}

	/**
	 * Retrieves and removes the unbound output gate with the given ID from the list of unbound output gates.
	 * 
	 * @param gateID
	 *        the index of the unbound output gate
	 * @return the unbound output gate with the given ID, or <code>null</code> if no such gate exists
	 */
	public OutputGate<? extends Record> getUnboundOutputGate(int gateID) {

		if (this.unboundOutputGates.size() == 0) {
			LOG.debug("No unbound output gates");
			return null;
		}
		return this.unboundOutputGates.remove(gateID);
	}

	/**
	 * Retrieves and removes unbound input gate with the given ID from the list of unbound input gates.
	 * 
	 * @param gateID
	 *        the index of the unbound input gate
	 * @return the unbound input gate with the given ID, or <code>null</code> if no such gate exists
	 */
	public InputGate<? extends Record> getUnboundInputGate(int gateID) {

		if (this.unboundInputGates.size() == 0) {
			LOG.debug("No unbound input gates");
			return null;
		}

		return this.unboundInputGates.remove(gateID);
	}

	/**
	 * Creates a new instance of the Nephele task and registers it with its
	 * environment.
	 */
	public void instantiateInvokable() {

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

		this.invokable.setEnvironment(this);
		this.invokable.registerInputOutput();

		if (this.jobID == null) {
			LOG.warn("jobVertexID is null");
		}

		// Set the vertex ID for all the gates
		for (int i = 0; i < this.inputGates.size(); i++) {
			this.inputGates.get(i).setJobID(this.jobID);
		}

		for (int i = 0; i < this.outputGates.size(); i++) {
			this.outputGates.get(i).setJobID(this.jobID);
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

		try {
			this.invokable.invoke();

			// Make sure we switch to the right state, even if the user code has not hit an {@link InterruptedException}
			if (this.executingThread.isInterrupted()) {
				throw new InterruptedException();
			}

		} catch (InterruptedException e) {
			changeExecutionState(ExecutionState.CANCELLED, null);
			// TODO: Do we really have to return here? (Rethink clean up strategy)
			return;
		} catch (Exception e) {
			// Report exception
			changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
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
			// Report exception
			changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			return;
		}

		// Finally, switch execution state to FINISHED and report to job manager
		changeExecutionState(ExecutionState.FINISHED, null);
	}

	/**
	 * Registers an output gate with the environment.
	 * 
	 * @param outputGate
	 *        the output gate to be registered with the environment
	 */
	public void registerOutputGate(OutputGate<? extends Record> outputGate) {
		LOG.debug("Registering output gate");
		this.outputGates.add(outputGate);
	}

	/**
	 * Registers an input gate with the environment.
	 * 
	 * @param inputGate
	 *        the input gate to be registered with the environment
	 */
	public void registerInputGate(InputGate<? extends Record> inputGate) {
		LOG.debug("Registering input gate");
		this.inputGates.add(inputGate);
	}

	/**
	 * Returns the number of output gates registered with this environment.
	 * 
	 * @return the number of output gates registered with this environment
	 */
	public int getNumberOfOutputGates() {
		return this.outputGates.size();
	}

	/**
	 * Returns the number of input gates registered with this environment.
	 * 
	 * @return the number of input gates registered with this environment
	 */
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
	public InputGate<? extends Record> getInputGate(int pos) {
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
	public OutputGate<? extends Record> getOutputGate(int pos) {
		if (pos < this.outputGates.size()) {
			return this.outputGates.get(pos);
		}

		return null;
	}

	/**
	 * Creates a new thread for the Nephele task and starts it.
	 */
	public void startExecution() {

		if (this.executingThread == null) {
			this.executingThread = new Thread(this, this.taskName);
			this.executingThread.start();
		}
	}

	/**
	 * Cancels the execution of the task (i.e. interrupts the execution thread).
	 */
	public void cancelExecution() {

		if (this.executingThread == null) {
			LOG.error("stopExecution called without having created an execution thread before");
			return;
		}

		// Interrupt the executing thread
		this.executingThread.interrupt();

		// Change state
		changeExecutionState(ExecutionState.CANCELLING, null);
	}

	// TODO: See if type safety can be improved here
	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {

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
			final String gateClassName = StringRecord.readString(in);
			Class<? extends Record> c = null;
			try {
				c = (Class<? extends Record>) Class.forName(gateClassName, true, cl);
			} catch (ClassNotFoundException cnfe) {
				throw new IOException("Class " + gateClassName + " not found in one of the supplied jar files: "
					+ StringUtils.stringifyException(cnfe));
			}
			final OutputGate<? extends Record> eog = new OutputGate(c, i);
			eog.read(in);
			this.outputGates.add(eog);
			// Mark as unbound for reconnection of RecordWriter
			this.unboundOutputGates.add(eog);
		}

		final int numInputGates = in.readInt();

		for (int i = 0; i < numInputGates; i++) {

			// TODO (erik) : gate.read(...) deserializes the type c anyway ...
			final InputGate<? extends Record> eig = new InputGate(null /* c */, i, null);
			eig.read(in);
			this.inputGates.add(eig);
			// Mark as unbound for reconnection of RecordReader
			this.unboundInputGates.add(eig);
		}

		// Read input splits
		final int numInputSplits = in.readInt();
		for (int i = 0; i < numInputSplits; i++) {
			final boolean isNotNull = in.readBoolean();
			if (isNotNull) {
				final String className = StringRecord.readString(in);
				Class<? extends IOReadableWritable> c = null;
				try {
					c = (Class<? extends IOReadableWritable>) Class.forName(className, true, cl);
				} catch (ClassNotFoundException cnfe) {
					throw new IOException("Class " + className + " not found in one of the supplied jar files: "
						+ StringUtils.stringifyException(cnfe));
				}

				try {
					final InputSplit inputSplit = (InputSplit) c.newInstance();
					inputSplit.read(in);
					this.inputSplits.add(inputSplit);
				} catch (InstantiationException e) {
					throw new IOException(e);
				} catch (IllegalAccessException e) {
					throw new IOException(e);
				}
			}
		}

		// The configuration object
		this.runtimeConfiguration = new Configuration();
		this.runtimeConfiguration.read(in);

		// The current of number subtasks
		this.currentNumberOfSubtasks = in.readInt();
		// The index in the subtask group
		this.indexInSubtaskGroup = in.readInt();
		// The execution state
		this.executionState = EnumUtils.readEnum(in, ExecutionState.class);

		// Finally, instantiate the invokable object
		instantiateInvokable();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

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
		out.writeInt(getNumberOfOutputGates());
		for (int i = 0; i < getNumberOfOutputGates(); i++) {
			StringRecord.writeString(out, getOutputGate(i).getType().getName());
			getOutputGate(i).write(out);
		}

		// Input gates
		out.writeInt(getNumberOfInputGates());
		for (int i = 0; i < getNumberOfInputGates(); i++) {
			getInputGate(i).write(out);
		}

		// Write out number of input splits
		out.writeInt(this.inputSplits.size());
		for (int i = 0; i < this.inputSplits.size(); i++) {
			final InputSplit inputSplit = this.inputSplits.get(i);
			if (inputSplit == null) {
				out.writeBoolean(false);
			} else {
				out.writeBoolean(true);
				StringRecord.writeString(out, inputSplit.getClass().getName());
				inputSplit.write(out);
			}
		}

		// The configuration object
		this.runtimeConfiguration.write(out);

		// The current of number subtasks
		out.writeInt(this.currentNumberOfSubtasks);
		// The index in the subtask group
		out.writeInt(this.indexInSubtaskGroup);
		// The execution state
		EnumUtils.writeEnum(out, this.executionState);
	}

	/**
	 * Blocks until all output channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the output channels
	 */
	private void waitForOutputChannelsToBeClosed() throws IOException {

		// Wait for disconnection of all output gates
		while (true) {

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfOutputGates(); i++) {
				final OutputGate<? extends Record> eog = getOutputGate(i);
				if (!eog.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				try {
					Thread.sleep(SLEEPINTERVAL);
				} catch (InterruptedException e) {
					LOG.debug(e);
				}
			}
		}
	}

	/**
	 * Blocks until all input channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the input channels
	 */
	private void waitForInputChannelsToBeClosed() throws IOException {

		// Wait for disconnection of all output gates
		while (true) {

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfInputGates(); i++) {
				final InputGate<? extends Record> eig = getInputGate(i);
				if (!eig.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				try {
					Thread.sleep(SLEEPINTERVAL);
				} catch (InterruptedException e) {
					LOG.debug(e);
				}
			}
		}
	}

	/**
	 * Closes all input gates which are not already closed.
	 */
	private void closeInputGates() throws IOException {

		for (int i = 0; i < getNumberOfInputGates(); i++) {
			final InputGate<? extends Record> eig = getInputGate(i);
			// Important: close must be called on each input gate exactly once
			eig.close();
		}

	}

	/**
	 * Requests all output gates to be closed.
	 */
	private void requestAllOutputGatesToClose() throws IOException {

		for (int i = 0; i < getNumberOfOutputGates(); i++) {
			this.getOutputGate(i).requestClose();
		}
	}

	/**
	 * Adds an input to the environment.
	 * 
	 * @param inputSplit
	 *        the input split to be added
	 */
	public void addInputSplit(InputSplit inputSplit) {

		this.inputSplits.add(inputSplit);
	}

	/**
	 * Returns a list of input splits assigned to this environment.
	 * 
	 * @return a (possibly empty) list of input splits assigned to this environment
	 */
	public InputSplit[] getInputSplits() {

		return this.inputSplits.toArray(new InputSplit[0]);
	}

	/**
	 * Returns the number of input splits assigned to this environment.
	 * 
	 * @return the number of input splits assigned to this environment
	 */
	public int getNumberOfInputSplits() {
		return this.inputSplits.size();
	}

	/**
	 * Returns a duplicate (deep copy) of this environment object. However, duplication
	 * does not cover the gates arrays. They must be manually reconstructed.
	 * 
	 * @return a duplicate (deep copy) of this environment object
	 */
	public Environment duplicateEnvironment() {

		final Environment duplicatedEnvironment = new Environment();
		duplicatedEnvironment.invokableClass = this.invokableClass;
		duplicatedEnvironment.executionState = this.executionState;
		// Input splits should be immutable, so we do not create deep copies of them
		duplicatedEnvironment.inputSplits.clear();
		final Iterator<InputSplit> it = this.inputSplits.iterator();
		while (it.hasNext()) {
			duplicatedEnvironment.inputSplits.add(it.next());
		}
		duplicatedEnvironment.jobID = this.jobID;
		duplicatedEnvironment.taskName = this.taskName;
		duplicatedEnvironment.executingThread = this.executingThread;
		final Iterator<ExecutionListener> it2 = this.executionListeners.iterator();
		while (it2.hasNext()) {
			duplicatedEnvironment.executionListeners.add(it2.next());
		}
		duplicatedEnvironment.runtimeConfiguration = this.runtimeConfiguration;

		// We instantiate the invokable of the new environment
		duplicatedEnvironment.instantiateInvokable();

		return duplicatedEnvironment;
	}

	/**
	 * Returns the current {@link IOManager}.
	 * 
	 * @return the current {@link IOManager}.
	 */
	public IOManager getIOManager() {
		return this.ioManager;
	}

	/**
	 * Sets the {@link IOManager}.
	 * 
	 * @param memoryManager
	 *        the new {@link IOManager}
	 */
	public void setIOManager(IOManager ioManager) {
		this.ioManager = ioManager;
	}

	/**
	 * Returns the current {@link MemoryManager}.
	 * 
	 * @return the current {@link MemoryManager}.
	 */
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	/**
	 * Sets the {@link MemoryManager}.
	 * 
	 * @param memoryManager
	 *        the new {@link MemoryManager}
	 */
	public void setMemoryManager(MemoryManager memoryManager) {
		this.memoryManager = memoryManager;
	}

	/**
	 * Returns the runtime configuration object which was attached to the original {@link JobVertex}.
	 * 
	 * @return the runtime configuration object which was attached to the original {@link JobVertex}
	 */
	public Configuration getRuntimeConfiguration() {
		return this.runtimeConfiguration;
	}

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 * 
	 * @return the current number of subtasks the respective task is split into
	 */
	public int getCurrentNumberOfSubtasks() {

		return this.currentNumberOfSubtasks;
	}

	/**
	 * Sets the current number of subtasks the respective task is split into.
	 * 
	 * @param currentNumberOfSubtasks
	 *        the current number of subtasks the respective task is split into
	 */
	public void setCurrentNumberOfSubtasks(int currentNumberOfSubtasks) {

		this.currentNumberOfSubtasks = currentNumberOfSubtasks;
	}

	/**
	 * Returns the index of this subtask in the subtask group.
	 * 
	 * @return the index of this subtask in the subtask group
	 */
	public int getIndexInSubtaskGroup() {

		return this.indexInSubtaskGroup;
	}

	/**
	 * Sets the index of this subtask in the subtask group.
	 * 
	 * @param indexInSubtaskGroup
	 *        the index of this subtask in the subtask group
	 */
	public void setIndexInSubtaskGroup(int indexInSubtaskGroup) {

		this.indexInSubtaskGroup = indexInSubtaskGroup;
	}

	public void changeExecutionState(ExecutionState newExecutionState, String optionalMessage) {

		LOG.info("ExecutionState set from " + executionState + " to " + newExecutionState + " for task "
			+ this.getTaskName() + " (" + (this.getIndexInSubtaskGroup() + 1) + "/" + this.getCurrentNumberOfSubtasks()
			+ ")");

		boolean unexpectedStateChange = true;

		// This is the regular life cycle of a task
		if (this.executionState == ExecutionState.CREATED && newExecutionState == ExecutionState.SCHEDULED) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.SCHEDULED && newExecutionState == ExecutionState.ASSIGNING) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.ASSIGNING && newExecutionState == ExecutionState.ASSIGNED) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.ASSIGNED && newExecutionState == ExecutionState.READY) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.READY && newExecutionState == ExecutionState.RUNNING) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.RUNNING && newExecutionState == ExecutionState.FINISHING) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.FINISHING && newExecutionState == ExecutionState.FINISHED) {
			unexpectedStateChange = false;
		}

		// The following error cases are not unexpected
		if (this.executionState == ExecutionState.SCHEDULED && newExecutionState == ExecutionState.ASSIGNED) {
			/**
			 * This transition can appear if an instance is used in different stages. In this case
			 * the respective vertices in the upper stages may already be assigned an instance although the
			 * scheduler did not explicitly ask for it.
			 */
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.SCHEDULED && newExecutionState == ExecutionState.CANCELLED) {
			/**
			 * This transition can appear if a task in a stage which is not yet executed gets canceled.
			 */
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.ASSIGNING && newExecutionState == ExecutionState.CANCELLED) {
			/**
			 * This transition can appear if a task is canceled after an instance request has been triggered.
			 */
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.ASSIGNED && newExecutionState == ExecutionState.CANCELLED) {
			/**
			 * This transition can appear if a task is canceled after an instance request has been triggered.
			 */
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.READY && newExecutionState == ExecutionState.CANCELLED) {
			/**
			 * This transition can appear if a task is canceled that is not yet running on the task manager.
			 */
			unexpectedStateChange = false;
		}

		if (this.executionState == ExecutionState.RUNNING && newExecutionState == ExecutionState.FAILED) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.FINISHING && newExecutionState == ExecutionState.FAILED) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.RUNNING && newExecutionState == ExecutionState.CANCELLING) {
			unexpectedStateChange = false;
		}
		if (this.executionState == ExecutionState.CANCELLING && newExecutionState == ExecutionState.CANCELLED) {
			unexpectedStateChange = false;
		}

		if (unexpectedStateChange) {
			LOG.error("Unexpected state change: " + this.executionState + " -> " + newExecutionState);
		}

		this.executionState = newExecutionState;

		// Notify all the observers
		synchronized (this.executionListeners) {
			final Iterator<ExecutionListener> it = this.executionListeners.iterator();
			while (it.hasNext()) {
				it.next().executionStateChanged(this, newExecutionState, optionalMessage);
			}

		}
	}

	/**
	 * Returns the thread which currently executes the assigned task.
	 * 
	 * @return the thread executing the assigned task
	 */
	public Thread getExecutingThread() {
		return this.executingThread;
	}

	/**
	 * Returns the name of the task running in this environment.
	 * 
	 * @return the name of the task running in this environment
	 */
	public String getTaskName() {

		return this.taskName;
	}

	/**
	 * Sends a notification to all registered {@link ExecutionListener} objects that a new user thread has been
	 * started.
	 * 
	 * @param userThread
	 *        the user thread which has been started
	 */
	public void userThreadStarted(Thread userThread) {

		synchronized (this.executionListeners) {
			final Iterator<ExecutionListener> it = this.executionListeners.iterator();
			while (it.hasNext()) {
				it.next().userThreadStarted(this, userThread);
			}
		}

	}

	/**
	 * Sends a notification to all registered {@link ExecutionListener} objects that a user thread has finished.
	 * 
	 * @param userThread
	 *        the user thread which has finished
	 */
	public void userThreadFinished(Thread userThread) {

		synchronized (this.executionListeners) {
			final Iterator<ExecutionListener> it = this.executionListeners.iterator();
			while (it.hasNext()) {
				it.next().userThreadFinished(this, userThread);
			}
		}
	}
}
