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

package eu.stratosphere.nephele.profiling.impl;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.profiling.ProfilingUtils;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.profiling.impl.types.InternalExecutionVertexThreadProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalInputGateProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalOutputGateProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.ProfilingDataContainer;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public class TaskManagerProfilerImpl extends TimerTask implements TaskManagerProfiler {

	private static final Log LOG = LogFactory.getLog(TaskManagerProfilerImpl.class);

	private final ProfilerImplProtocol jobManagerProfiler;

	private final Timer timer;

	private final ThreadMXBean tmx;

	private final long timerInterval;

	private final ProfilingDataContainer profilingDataContainer = new ProfilingDataContainer();

	private final InstanceProfiler instanceProfiler;

	private final Map<Environment, EnvironmentThreadSet> monitoredThreads = new HashMap<Environment, EnvironmentThreadSet>();

	private final Map<InputGate<? extends Record>, InputGateListenerImpl> monitoredInputGates = new HashMap<InputGate<? extends Record>, InputGateListenerImpl>();

	private final Map<OutputGate<? extends Record>, OutputGateListenerImpl> monitoredOutputGates = new HashMap<OutputGate<? extends Record>, OutputGateListenerImpl>();

	public TaskManagerProfilerImpl(InetAddress jobManagerAddress, InstanceConnectionInfo instanceConnectionInfo)
																												throws ProfilingException {

		// Create RPC stub for communication with job manager's profiling component.
		final InetSocketAddress profilingAddress = new InetSocketAddress(jobManagerAddress, GlobalConfiguration
			.getInteger(ProfilingUtils.JOBMANAGER_RPC_PORT_KEY, ProfilingUtils.JOBMANAGER_DEFAULT_RPC_PORT));
		ProfilerImplProtocol jobManagerProfilerTmp = null;
		try {
			jobManagerProfilerTmp = (ProfilerImplProtocol) RPC.getProxy(ProfilerImplProtocol.class, profilingAddress,
				NetUtils.getSocketFactory());
		} catch (IOException e) {
			throw new ProfilingException(StringUtils.stringifyException(e));
		}
		this.jobManagerProfiler = jobManagerProfilerTmp;

		// Initialize MX interface and check if thread contention monitoring is supported
		this.tmx = ManagementFactory.getThreadMXBean();
		if (this.tmx.isThreadContentionMonitoringSupported()) {
			this.tmx.setThreadContentionMonitoringEnabled(true);
		} else {
			throw new ProfilingException("The thread contention monitoring is not supported.");
		}

		// Create instance profiler
		this.instanceProfiler = new InstanceProfiler(instanceConnectionInfo);

		// Set and trigger timer
		this.timerInterval = (long) (GlobalConfiguration.getInteger(ProfilingUtils.TASKMANAGER_REPORTINTERVAL_KEY,
			ProfilingUtils.DEFAULT_TASKMANAGER_REPORTINTERVAL) * 1000);
		// The initial delay is based on a random value, so the task managers will not send data to the job manager all
		// at once.
		final long initialDelay = (long) (Math.random() * this.timerInterval);
		this.timer = new Timer(true);
		this.timer.schedule(this, initialDelay, this.timerInterval);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerExecutionListener(ExecutionVertexID id, Configuration jobConfiguration,
			Environment environment) {

		// Register profiling hook for the environment
		environment.registerExecutionListener(new EnvironmentListenerImpl(this, id, environment, this.timerInterval));
	}

	@Override
	public void registerInputGateListener(ExecutionVertexID id, Configuration jobConfiguration,
			InputGate<? extends Record> inputGate) {

		synchronized (this.monitoredInputGates) {

			final InputGateListenerImpl inputGateListener = new InputGateListenerImpl(inputGate.getJobID(), id,
				inputGate.getIndex());
			inputGate.registerInputGateListener(inputGateListener);
			this.monitoredInputGates.put(inputGate, inputGateListener);
		}
	}

	@Override
	public void registerOutputGateListener(ExecutionVertexID id, Configuration jobConfiguration,
			OutputGate<? extends Record> outputGate) {

		synchronized (this.monitoredOutputGates) {

			final OutputGateListenerImpl outputGateListener = new OutputGateListenerImpl(outputGate.getJobID(), id,
				outputGate.getIndex());
			outputGate.registerOutputGateListener(outputGateListener);
			this.monitoredOutputGates.put(outputGate, outputGateListener);
		}
	}

	@Override
	public void unregisterExecutionListener(ExecutionVertexID id) {
		/*
		 * Nothing to do here, the task will unregister itself when its
		 * execution state has either switched to FINISHED, CANCELLED,
		 * or FAILED.
		 */
	}

	@Override
	public void unregisterInputGateListeners(ExecutionVertexID id) {

		synchronized (this.monitoredInputGates) {

			final Iterator<InputGate<? extends Record>> it = this.monitoredInputGates.keySet().iterator();
			while (it.hasNext()) {

				final InputGate<? extends Record> inputGate = it.next();
				if (this.monitoredInputGates.get(inputGate).getExecutionVertexID().equals(id)) {
					it.remove();
				}
			}
		}
	}

	@Override
	public void unregisterOutputGateListeners(ExecutionVertexID id) {

		synchronized (this.monitoredOutputGates) {

			final Iterator<OutputGate<? extends Record>> it = this.monitoredOutputGates.keySet().iterator();
			while (it.hasNext()) {

				final OutputGate<? extends Record> outputGate = it.next();
				if (this.monitoredOutputGates.get(outputGate).getExecutionVertexID().equals(id)) {
					it.remove();
				}
			}
		}
	}

	@Override
	public void shutdown() {

		// Stop the timer task
		this.timer.cancel();
	}

	@Override
	public void run() {

		final long timestamp = System.currentTimeMillis();
		InternalInstanceProfilingData instanceProfilingData = null;

		// Collect profiling information of the threads
		synchronized (this.monitoredThreads) {

			final Iterator<Environment> iterator = this.monitoredThreads.keySet().iterator();
			while (iterator.hasNext()) {
				final Environment environment = iterator.next();
				final EnvironmentThreadSet environmentThreadSet = this.monitoredThreads.get(environment);
				final InternalExecutionVertexThreadProfilingData threadProfilingData = environmentThreadSet
					.captureCPUUtilization(environment.getJobID(), this.tmx, timestamp);
				if (threadProfilingData != null) {
					this.profilingDataContainer.addProfilingData(threadProfilingData);
				}
			}

			// If there is at least one registered environment, also create an instance profiling object
			if (!this.monitoredThreads.isEmpty()) {
				try {
					instanceProfilingData = this.instanceProfiler.generateProfilingData(timestamp);
				} catch (ProfilingException e) {
					LOG.error("Error while retrieving instance profiling data: ", e);
				}
			}
		}

		// Collect profiling information of the input gates
		synchronized (this.monitoredInputGates) {

			final Iterator<InputGate<? extends Record>> iterator = this.monitoredInputGates.keySet().iterator();
			while (iterator.hasNext()) {

				final InputGate<? extends Record> inputGate = iterator.next();
				final InputGateListenerImpl listener = this.monitoredInputGates.get(inputGate);
				this.profilingDataContainer.addProfilingData(new InternalInputGateProfilingData(listener.getJobID(),
					listener.getExecutionVertexID(), listener.getGateIndex(), (int) timerInterval, listener
						.getAndResetCounter()));
			}
		}

		synchronized (this.monitoredOutputGates) {

			final Iterator<OutputGate<? extends Record>> iterator = this.monitoredOutputGates.keySet().iterator();
			while (iterator.hasNext()) {

				final OutputGate<? extends Record> outputGate = iterator.next();
				final OutputGateListenerImpl listener = this.monitoredOutputGates.get(outputGate);
				this.profilingDataContainer.addProfilingData(new InternalOutputGateProfilingData(listener.getJobID(),
					listener.getExecutionVertexID(), listener.getGateIndex(), (int) timerInterval, listener
						.getAndResetCounter()));
			}

		}

		// Send all queued profiling records to the job manager and clear container
		synchronized (this.profilingDataContainer) {

			if (instanceProfilingData != null) {
				this.profilingDataContainer.addProfilingData(instanceProfilingData);
			}

			if (!this.profilingDataContainer.isEmpty()) {
				try {
					this.jobManagerProfiler.reportProfilingData(this.profilingDataContainer);
					this.profilingDataContainer.clear();
				} catch (IOException e) {
					LOG.error(e);
				}
			}
		}
	}

	public void registerMainThreadForCPUProfiling(Environment environment, Thread thread,
			ExecutionVertexID executionVertexID) {

		synchronized (this.monitoredThreads) {
			LOG.debug("Registering thread " + thread.getName() + " for CPU monitoring");
			if (this.monitoredThreads.containsKey(environment)) {
				LOG.error("There is already a main thread registered for environment object "
					+ environment.getTaskName());
			}

			this.monitoredThreads.put(environment, new EnvironmentThreadSet(this.tmx, thread, executionVertexID));
		}
	}

	public void registerUserThreadForCPUProfiling(Environment environment, Thread userThread) {

		synchronized (this.monitoredThreads) {

			final EnvironmentThreadSet environmentThreadList = this.monitoredThreads.get(environment);
			if (environmentThreadList == null) {
				LOG.error("Trying to register " + userThread.getName() + " but no main thread found!");
				return;
			}

			environmentThreadList.addUserThread(this.tmx, userThread);
		}

	}

	public void unregisterMainThreadFromCPUProfiling(Environment environment, Thread thread) {

		synchronized (this.monitoredThreads) {
			LOG.debug("Unregistering thread " + thread.getName() + " from CPU monitoring");
			final EnvironmentThreadSet environmentThreadSet = this.monitoredThreads.remove(environment);
			if (environmentThreadSet != null) {

				if (environmentThreadSet.getMainThread() != thread) {
					LOG.error("The thread " + thread.getName() + " is not the main thread of this environment");
				}

				if (environmentThreadSet.getNumberOfUserThreads() > 0) {
					LOG.error("Thread " + environmentThreadSet.getMainThread().getName()
						+ " has still unfinished user threads!");
				}
			}
		}
	}

	public void unregisterUserThreadFromCPUProfiling(Environment environment, Thread userThread) {

		synchronized (this.monitoredThreads) {

			final EnvironmentThreadSet environmentThreadSet = this.monitoredThreads.get(environment);
			if (environmentThreadSet == null) {
				LOG.error("Trying to unregister " + userThread.getName() + " but no main thread found!");
				return;
			}

			environmentThreadSet.removeUserThread(userThread);
		}

	}

	/*
	 * public void publishProfilingData(InternalProfilingData profilingData) {
	 * this.profilingDataContainer.addProfilingData(profilingData);
	 * }
	 */

}
