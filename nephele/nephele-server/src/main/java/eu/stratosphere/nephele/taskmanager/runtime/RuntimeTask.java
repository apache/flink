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

package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.annotations.Stateful;
import eu.stratosphere.nephele.annotations.Stateless;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ExecutionStateTransition;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class RuntimeTask implements Task, ExecutionObserver {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(RuntimeTask.class);

	private static final long NANO_TO_MILLISECONDS = 1000 * 1000;

	private final ExecutionVertexID vertexID;

	private final RuntimeEnvironment environment;

	private final TaskManager taskManager;

	/**
	 * Stores whether the task has been canceled.
	 */
	private volatile boolean isCanceled = false;

	/**
	 * The current execution state of the task
	 */
	private volatile ExecutionState executionState = ExecutionState.STARTING;

	private Queue<ExecutionListener> registeredListeners = new ConcurrentLinkedQueue<ExecutionListener>();

	private long startTime;

	// DW: Start of temporary code
	private double pactInputOutputRatioSum = 0.0;
	
	private int numberOfPactInputOutputRatioEntries = 0;
	// DW: End of temporay code
	
	public RuntimeTask(final ExecutionVertexID vertexID, final RuntimeEnvironment environment,
			final TaskManager taskManager) {

		this.vertexID = vertexID;
		this.environment = environment;
		this.taskManager = taskManager;

		this.environment.setExecutionObserver(this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(final ExecutionState newExecutionState, final String optionalMessage) {

		// Check the state transition
		ExecutionStateTransition.checkTransition(false, getTaskName(), this.executionState, newExecutionState);

		// Make sure the reason for a transition to FAILED appears in the log files
		if (newExecutionState == ExecutionState.FAILED) {
			LOG.error(optionalMessage);
		}

		// Notify all listener objects
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().executionStateChanged(this.environment.getJobID(), this.vertexID, newExecutionState,
				optionalMessage);
		}

		// Store the new execution state
		this.executionState = newExecutionState;

		// Finally propagate the state change to the job manager
		this.taskManager.executionStateChanged(this.environment.getJobID(), this.vertexID, newExecutionState,
			optionalMessage);
	}

	/**
	 * Returns the name of the task associated with this observer object.
	 * 
	 * @return the name of the task associated with this observer object
	 */
	private String getTaskName() {

		return this.environment.getTaskName() + " (" + (this.environment.getIndexInSubtaskGroup() + 1) + "/"
			+ this.environment.getCurrentNumberOfSubtasks() + ")";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		// Notify the listeners
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().userThreadStarted(this.environment.getJobID(), this.vertexID, userThread);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Thread userThread) {

		// Notify the listeners
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().userThreadFinished(this.environment.getJobID(), this.vertexID, userThread);
		}
	}

	/**
	 * Registers the {@link ExecutionListener} object for this task. This object
	 * will be notified about important events during the task execution.
	 * 
	 * @param executionListener
	 *        the object to be notified for important events during the task execution
	 */

	public void registerExecutionListener(final ExecutionListener executionListener) {

		this.registeredListeners.add(executionListener);
	}

	/**
	 * Unregisters the {@link ExecutionListener} object for this environment. This object
	 * will no longer be notified about important events during the task execution.
	 * 
	 * @param executionListener
	 *        the lister object to be unregistered
	 */

	public void unregisterExecutionListener(final ExecutionListener executionListener) {

		this.registeredListeners.remove(executionListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void markAsFailed() {

		executionStateChanged(ExecutionState.FAILED, "Execution thread died unexpectedly");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cancelExecution() {

		cancelOrKillExecution(true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killExecution() {

		cancelOrKillExecution(false);
	}

	/**
	 * Cancels or kills the task.
	 * 
	 * @param cancel
	 *        <code>true/code> if the task shall be cancelled, <code>false</code> if it shall be killed
	 */
	private void cancelOrKillExecution(final boolean cancel) {

		final Thread executingThread = this.environment.getExecutingThread();

		if (executingThread == null) {
			return;
		}

		if (this.executionState != ExecutionState.RUNNING && this.executionState != ExecutionState.REPLAYING
			&& this.executionState != ExecutionState.FINISHING) {
			return;
		}

		LOG.info((cancel ? "Canceling " : "Killing ") + this.environment.getTaskNameWithIndex());

		if (cancel) {
			this.isCanceled = true;
			// Change state
			executionStateChanged(ExecutionState.CANCELING, null);
		}

		// Request user code to shut down
		try {
			final AbstractInvokable invokable = this.environment.getInvokable();
			if (invokable != null) {
				invokable.cancel();
			}
		} catch (Throwable e) {
			LOG.error(StringUtils.stringifyException(e));
		}

		// Continuously interrupt the user thread until it changed to state CANCELED
		while (true) {

			executingThread.interrupt();

			if (!executingThread.isAlive()) {
				break;
			}

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				break;
			}

			LOG.info((cancel == true ? "Canceling " : "Killing ") + this.environment.getTaskName()
				+ " with state " + this.executionState);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void startExecution() {

		final Thread thread = this.environment.getExecutingThread();
		thread.start();
		this.startTime = System.currentTimeMillis();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isCanceled() {

		return this.isCanceled;
	}

	/**
	 * Triggers the notification that the task has run out of its initial execution resources.
	 */
	public void initialExecutionResourcesExhausted() {

		System.out.println("PACT input/output for task " + this.environment.getTaskNameWithIndex() + ": " + getPACTInputOutputRatio());
		
		// if (this.environment.getExecutingThread() != Thread.currentThread()) {
		// throw new ConcurrentModificationException(
		// "initialExecutionResourcesExhausted must be called from the task that executes the user code");
		// }

		// Construct a resource utilization snapshot
		final long timestamp = System.currentTimeMillis();
		if (this.environment.getInputGate(0) != null
			&& this.environment.getInputGate(0).getExecutionStart() < timestamp) {
			this.startTime = this.environment.getInputGate(0).getExecutionStart();
		}
		LOG.info("Task " + this.getTaskName() + " started " + this.startTime);
		// Get CPU-Usertime in percent
		ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
		long userCPU = (threadBean.getCurrentThreadUserTime() / NANO_TO_MILLISECONDS) * 100
			/ (timestamp - this.startTime);
		LOG.info("USER CPU for " + this.getTaskName() + " : " + userCPU);
		// collect outputChannelUtilization
		final Map<ChannelID, Long> channelUtilization = new HashMap<ChannelID, Long>();
		long totalOutputAmount = 0;
		int numrec = 0;
		long averageOutputRecordSize= 0;
		for (int i = 0; i < this.environment.getNumberOfOutputGates(); ++i) {
			final OutputGate<? extends Record> outputGate = this.environment.getOutputGate(i);
			 numrec += outputGate.getNumRecords();
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
				channelUtilization.put(outputChannel.getID(),
					Long.valueOf(outputChannel.getAmountOfDataTransmitted()));
				totalOutputAmount += outputChannel.getAmountOfDataTransmitted();
			}
		}

		if(numrec != 0){
			averageOutputRecordSize = totalOutputAmount/numrec;
		}
		//FIXME (marrus) it is not about what we received but what we processed yet
		boolean allClosed = true;
		int numinrec = 0;

		long totalInputAmount = 0;
		long averageInputRecordSize = 0;
		for (int i = 0; i < this.environment.getNumberOfInputGates(); ++i) {
			final InputGate<? extends Record> inputGate = this.environment.getInputGate(i);
			numinrec += inputGate.getNumRecords();
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
				final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(j);
				channelUtilization.put(inputChannel.getID(),
					Long.valueOf(inputChannel.getAmountOfDataTransmitted()));
				totalInputAmount += inputChannel.getAmountOfDataTransmitted();
				try {
					if(!inputChannel.isClosed()){
						allClosed = false;
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
		if(numinrec != 0){
			averageInputRecordSize = totalInputAmount/numinrec;
		}
		Boolean force = null;
		Boolean stateful = false;
		if (this.environment.getInvokable().getClass().isAnnotationPresent(Stateful.class)
			&& !this.environment.getInvokable().getClass().isAnnotationPresent(Stateless.class)) {
			// Don't checkpoint stateful tasks
			force = false;
		} else {
			if(this.environment.getForced() != null){
				force = this.environment.getForced();
			}else{
			// look for a forced decision from the user
			ForceCheckpoint forced = this.environment.getInvokable().getClass().getAnnotation(ForceCheckpoint.class);
			
			//this.environment.getInvokable().getTaskConfiguration().getBoolean("forced_checkpoint", false)
		
			if (forced != null) {
				force = forced.checkpoint();
			}
			}
		}

		final ResourceUtilizationSnapshot rus = new ResourceUtilizationSnapshot(timestamp, channelUtilization, userCPU,
			force, totalInputAmount, totalOutputAmount, averageOutputRecordSize, averageInputRecordSize, getPACTInputOutputRatio(), allClosed);

		// Notify the listener objects
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().initialExecutionResourcesExhausted(this.environment.getJobID(), this.vertexID, rus);
		}

		// Finally, propagate event to the job manager
		this.taskManager.initialExecutionResourcesExhausted(this.environment.getJobID(), this.vertexID, rus);
	}

	public void checkpointStateChanged(final CheckpointState newCheckpointState) {

		// Propagate event to the job manager
		this.taskManager.checkpointStateChanged(this.environment.getJobID(), this.vertexID, newCheckpointState);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isTerminated() {

		final Thread executingThread = this.environment.getExecutingThread();
		if (executingThread.getState() == Thread.State.TERMINATED) {
			return true;
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Environment getEnvironment() {

		return this.environment;
	}

	/**
	 * Returns the runtime environment associated with this task.
	 * 
	 * @return the runtime environment associated with this task
	 */
	public RuntimeEnvironment getRuntimeEnvironment() {

		return this.environment;
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
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerMemoryManager(final MemoryManager memoryManager) {

		this.environment.setMemoryManager(memoryManager);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerIOManager(final IOManager ioManager) {

		this.environment.setIOManager(ioManager);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputSplitProvider(final InputSplitProvider inputSplitProvider) {

		this.environment.setInputSplitProvider(inputSplitProvider);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerProfiler(final TaskManagerProfiler taskManagerProfiler, final Configuration jobConfiguration) {

		taskManagerProfiler.registerExecutionListener(this, jobConfiguration);

		for (int i = 0; i < this.environment.getNumberOfInputGates(); i++) {
			taskManagerProfiler.registerInputGateListener(this.vertexID, jobConfiguration,
				this.environment.getInputGate(i));
		}

		for (int i = 0; i < this.environment.getNumberOfOutputGates(); i++) {
			taskManagerProfiler.registerOutputGateListener(this.vertexID, jobConfiguration,
				this.environment.getOutputGate(i));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterMemoryManager(final MemoryManager memoryManager) {

		if (memoryManager != null) {
			memoryManager.releaseAll(this.environment.getInvokable());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterProfiler(final TaskManagerProfiler taskManagerProfiler) {

		if (taskManagerProfiler != null) {
			taskManagerProfiler.unregisterOutputGateListeners(this.vertexID);
			taskManagerProfiler.unregisterInputGateListeners(this.vertexID);
			taskManagerProfiler.unregisterExecutionListener(this.vertexID);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TaskContext createTaskContext(final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final Map<ExecutionVertexID, RuntimeTaskContext> tasksWithUndecidedCheckpoints,
			final LocalBufferPoolOwner previousBufferPoolOwner) {

		if (previousBufferPoolOwner != null) {
			throw new IllegalStateException("Vertex " + this.vertexID + " has a previous buffer pool owner");
		}

		return new RuntimeTaskContext(this, transferEnvelopeDispatcher, tasksWithUndecidedCheckpoints);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionState getExecutionState() {

		return this.executionState;
	}

	// DW: Start of temporary code
	@Override
	public void reportPACTDataStatistics(final long numberOfConsumedBytes, final long numberOfProducedBytes) {
				
		this.pactInputOutputRatioSum += ((double) numberOfProducedBytes / (double) numberOfConsumedBytes);
		++this.numberOfPactInputOutputRatioEntries;
	}
	
	private double getPACTInputOutputRatio() {
		
		if(this.numberOfPactInputOutputRatioEntries == 0) {
			return -1.0;
		}
		
		return (this.pactInputOutputRatioSum / (double) this.numberOfPactInputOutputRatioEntries);
	}
	// DW: End of temporary code
	
}
