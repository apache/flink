/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.JobManagerProfiler;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.profiling.ProfilingListener;
import eu.stratosphere.nephele.profiling.ProfilingUtils;
import eu.stratosphere.nephele.profiling.impl.types.InternalExecutionVertexThreadProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalInputGateProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalOutputGateProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalProfilingData;
import eu.stratosphere.nephele.profiling.types.InputGateProfilingEvent;
import eu.stratosphere.nephele.profiling.types.InstanceSummaryProfilingEvent;
import eu.stratosphere.nephele.profiling.types.OutputGateProfilingEvent;
import eu.stratosphere.nephele.profiling.types.SingleInstanceProfilingEvent;
import eu.stratosphere.nephele.profiling.types.ThreadProfilingEvent;
import eu.stratosphere.nephele.rpc.ProfilingTypeUtils;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.util.StringUtils;

public class JobManagerProfilerImpl implements JobManagerProfiler, ProfilerImplProtocol {

	private static final Log LOG = LogFactory.getLog(JobManagerProfilerImpl.class);

	private final RPCService rpcService;

	private final Map<JobID, List<ProfilingListener>> registeredListeners = new HashMap<JobID, List<ProfilingListener>>();

	private final Map<JobID, JobProfilingData> registeredJobs = new HashMap<JobID, JobProfilingData>();

	public JobManagerProfilerImpl(InetAddress jobManagerbindAddress) throws ProfilingException {

		// Start profiling IPC server
		final int rpcPort = GlobalConfiguration.getInteger(ProfilingUtils.JOBMANAGER_RPC_PORT_KEY,
			ProfilingUtils.JOBMANAGER_DEFAULT_RPC_PORT);

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(rpcPort, 1, ProfilingTypeUtils.getRPCTypesToRegister());
		} catch (IOException ioe) {
			throw new ProfilingException("Cannot start profiling RPC server: " + StringUtils.stringifyException(ioe));
		}
		this.rpcService = rpcService;
		this.rpcService.setProtocolCallbackHandler(ProfilerImplProtocol.class, this);
	}

	@Override
	public void registerProfilingJob(ExecutionGraph executionGraph) {

		synchronized (this.registeredJobs) {
			this.registeredJobs.put(executionGraph.getJobID(), new JobProfilingData(executionGraph));
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterProfilingJob(ExecutionGraph executionGraph) {

		synchronized (this.registeredListeners) {
			this.registeredListeners.remove(executionGraph.getJobID());
		}

		synchronized (this.registeredJobs) {
			this.registeredJobs.remove(executionGraph.getJobID());
		}

	}

	@Override
	public void shutdown() {

		// Stop the RPC server
		if (this.rpcService != null) {
			LOG.debug("Stopping profiling RPC service");
			this.rpcService.shutDown();
		}
	}

	private void dispatchThreadData(long timestamp, InternalExecutionVertexThreadProfilingData profilingData) {

		final long profilingStart = getProfilingStart(profilingData.getJobID());
		if (profilingStart < 0 && LOG.isDebugEnabled()) {
			LOG.debug("Received profiling data for unregistered job " + profilingData.getJobID());
			return;
		}

		synchronized (this.registeredListeners) {

			final List<ProfilingListener> jobListeners = this.registeredListeners.get(profilingData.getJobID());
			if (jobListeners == null) {
				return;
			}

			final ThreadProfilingEvent threadProfilingEvent = new ThreadProfilingEvent(profilingData.getUserTime(),
				profilingData.getSystemTime(), profilingData.getBlockedTime(), profilingData.getWaitedTime(),
				profilingData.getExecutionVertexID().toManagementVertexID(), profilingData.getProfilingInterval(),
				profilingData.getJobID(), timestamp, (timestamp - profilingStart));

			final Iterator<ProfilingListener> it = jobListeners.iterator();
			while (it.hasNext()) {
				it.next().processProfilingEvents(threadProfilingEvent);
			}
		}
	}

	private void dispatchInstanceData(long timestamp, InternalInstanceProfilingData profilingData) {

		// Check which of the registered jobs are assigned to that instance
		synchronized (this.registeredJobs) {

			Iterator<JobID> it = this.registeredJobs.keySet().iterator();
			while (it.hasNext()) {
				final JobID jobID = it.next();
				final JobProfilingData jobProfilingData = this.registeredJobs.get(jobID);
				if (!jobProfilingData.instanceAllocatedByJob(profilingData)) {
					continue;
				}

				final SingleInstanceProfilingEvent singleInstanceProfilingEvent = new SingleInstanceProfilingEvent(
					profilingData.getProfilingInterval(), profilingData.getIOWaitCPU(), profilingData.getIdleCPU(),
					profilingData.getUserCPU(), profilingData.getSystemCPU(), profilingData.getHardIrqCPU(),
					profilingData.getSoftIrqCPU(), profilingData.getTotalMemory(), profilingData.getFreeMemory(),
					profilingData.getBufferedMemory(), profilingData.getCachedMemory(), profilingData
						.getCachedSwapMemory(), profilingData.getReceivedBytes(), profilingData.getTransmittedBytes(),
					jobID, timestamp, timestamp - jobProfilingData.getProfilingStart(), profilingData
						.getInstanceConnectionInfo().toString());

				synchronized (this.registeredListeners) {

					List<ProfilingListener> jobListeners = this.registeredListeners.get(jobID);
					if (jobListeners == null) {
						continue;
					}

					final InstanceSummaryProfilingEvent instanceSummary = jobProfilingData
						.getInstanceSummaryProfilingData(timestamp);

					final Iterator<ProfilingListener> listenerIterator = jobListeners.iterator();
					while (listenerIterator.hasNext()) {
						final ProfilingListener profilingListener = listenerIterator.next();
						profilingListener.processProfilingEvents(singleInstanceProfilingEvent);
						if (instanceSummary != null) {
							profilingListener.processProfilingEvents(instanceSummary);
						}
					}
				}
			}
		}
	}

	private void dispatchInputGateData(long timestamp, InternalInputGateProfilingData profilingData) {

		final long profilingStart = getProfilingStart(profilingData.getJobID());
		if (profilingStart < 0) {
			LOG.error("Received profiling data for unregistered job " + profilingData.getJobID());
			return;
		}

		synchronized (this.registeredListeners) {

			final List<ProfilingListener> jobListeners = this.registeredListeners.get(profilingData.getJobID());
			if (jobListeners == null) {
				return;
			}

			final InputGateProfilingEvent inputGateProfilingEvent = new InputGateProfilingEvent(profilingData
				.getGateIndex(), profilingData.getNoRecordsAvailableCounter(), profilingData.getExecutionVertexID()
				.toManagementVertexID(), profilingData.getProfilingInterval(), profilingData.getJobID(), timestamp,
				timestamp - profilingStart);

			final Iterator<ProfilingListener> it = jobListeners.iterator();
			while (it.hasNext()) {
				it.next().processProfilingEvents(inputGateProfilingEvent);
			}
		}
	}

	private void dispatchOutputGateData(long timestamp, InternalOutputGateProfilingData profilingData) {

		final long profilingStart = getProfilingStart(profilingData.getJobID());
		if (profilingStart < 0) {
			LOG.error("Received profiling data for unregistered job " + profilingData.getJobID());
			return;
		}

		synchronized (this.registeredListeners) {

			final List<ProfilingListener> jobListeners = this.registeredListeners.get(profilingData.getJobID());
			if (jobListeners == null) {
				return;
			}

			final OutputGateProfilingEvent outputGateProfilingEvent = new OutputGateProfilingEvent(profilingData
				.getGateIndex(), profilingData.getChannelCapacityExhaustedCounter(), profilingData
				.getExecutionVertexID().toManagementVertexID(), profilingData.getProfilingInterval(), profilingData
				.getJobID(), timestamp, timestamp - profilingStart);

			final Iterator<ProfilingListener> it = jobListeners.iterator();
			while (it.hasNext()) {
				it.next().processProfilingEvents(outputGateProfilingEvent);
			}
		}
	}

	private long getProfilingStart(JobID jobID) {

		synchronized (this.registeredJobs) {

			final JobProfilingData profilingData = this.registeredJobs.get(jobID);
			if (profilingData == null) {
				return -1;
			}

			return profilingData.getProfilingStart();
		}
	}

	@Override
	public void reportProfilingData(final List<InternalProfilingData> profilingData) {

		final long timestamp = System.currentTimeMillis();

		// Process the received profiling data
		final Iterator<InternalProfilingData> dataIterator = profilingData.iterator();
		while (dataIterator.hasNext()) {

			final InternalProfilingData internalProfilingData = dataIterator.next();

			if (internalProfilingData instanceof InternalExecutionVertexThreadProfilingData) {
				dispatchThreadData(timestamp, (InternalExecutionVertexThreadProfilingData) internalProfilingData);
			} else if (internalProfilingData instanceof InternalInstanceProfilingData) {
				dispatchInstanceData(timestamp, (InternalInstanceProfilingData) internalProfilingData);
			} else if (internalProfilingData instanceof InternalInputGateProfilingData) {
				dispatchInputGateData(timestamp, (InternalInputGateProfilingData) internalProfilingData);
			} else if (internalProfilingData instanceof InternalOutputGateProfilingData) {
				dispatchOutputGateData(timestamp, (InternalOutputGateProfilingData) internalProfilingData);
			} else {
				LOG.error("Received unknown profiling data: " + internalProfilingData.getClass().getName());
			}
		}
	}

	@Override
	public void registerForProfilingData(JobID jobID, ProfilingListener profilingListener) {

		synchronized (this.registeredListeners) {

			List<ProfilingListener> jobListeners = this.registeredListeners.get(jobID);
			if (jobListeners == null) {
				jobListeners = new ArrayList<ProfilingListener>();
				this.registeredListeners.put(jobID, jobListeners);
			}

			jobListeners.add(profilingListener);
		}

	}

	@Override
	public void unregisterFromProfilingData(JobID jobID, ProfilingListener profilingListener) {

		synchronized (this.registeredListeners) {

			List<ProfilingListener> jobListeners = this.registeredListeners.get(jobID);
			if (jobListeners == null) {
				return;
			}

			jobListeners.remove(profilingListener);

			if (jobListeners.isEmpty()) {
				this.registeredListeners.remove(jobID);
			}
		}
	}
}
