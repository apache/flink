/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.profiling.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.ipc.Server;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.profiling.JobManagerProfiler;
import org.apache.flink.runtime.profiling.ProfilingException;
import org.apache.flink.runtime.profiling.ProfilingListener;
import org.apache.flink.runtime.profiling.ProfilingUtils;
import org.apache.flink.runtime.profiling.impl.types.InternalExecutionVertexThreadProfilingData;
import org.apache.flink.runtime.profiling.impl.types.InternalInputGateProfilingData;
import org.apache.flink.runtime.profiling.impl.types.InternalInstanceProfilingData;
import org.apache.flink.runtime.profiling.impl.types.InternalOutputGateProfilingData;
import org.apache.flink.runtime.profiling.impl.types.InternalProfilingData;
import org.apache.flink.runtime.profiling.impl.types.ProfilingDataContainer;
import org.apache.flink.runtime.profiling.types.InputGateProfilingEvent;
import org.apache.flink.runtime.profiling.types.InstanceSummaryProfilingEvent;
import org.apache.flink.runtime.profiling.types.OutputGateProfilingEvent;
import org.apache.flink.runtime.profiling.types.SingleInstanceProfilingEvent;
import org.apache.flink.runtime.profiling.types.ThreadProfilingEvent;
import org.apache.flink.util.StringUtils;

public class JobManagerProfilerImpl implements JobManagerProfiler, ProfilerImplProtocol {

	private static final Log LOG = LogFactory.getLog(JobManagerProfilerImpl.class);

	private static final String RPC_NUM_HANDLER_KEY = "jobmanager.profiling.rpc.numhandler";

	private static final int DEFAULT_NUM_HANLDER = 3;

	private final Server profilingServer;

	private final Map<JobID, List<ProfilingListener>> registeredListeners = new HashMap<JobID, List<ProfilingListener>>();

	private final Map<JobID, JobProfilingData> registeredJobs = new HashMap<JobID, JobProfilingData>();

	public JobManagerProfilerImpl(InetAddress jobManagerbindAddress) throws ProfilingException {

		// Start profiling IPC server
		final int handlerCount = GlobalConfiguration.getInteger(RPC_NUM_HANDLER_KEY, DEFAULT_NUM_HANLDER);
		final int rpcPort = GlobalConfiguration.getInteger(ProfilingUtils.JOBMANAGER_RPC_PORT_KEY,
			ProfilingUtils.JOBMANAGER_DEFAULT_RPC_PORT);
		
		final InetSocketAddress rpcServerAddress = new InetSocketAddress(jobManagerbindAddress, rpcPort);
		Server profilingServerTmp = null;
		try {

			profilingServerTmp = RPC.getServer(this, rpcServerAddress.getHostName(), rpcServerAddress.getPort(),
				handlerCount);
			profilingServerTmp.start();
		} catch (IOException ioe) {
			throw new ProfilingException("Cannot start profiling RPC server: " + StringUtils.stringifyException(ioe));
		}
		this.profilingServer = profilingServerTmp;

	}

	@Override
	public void registerProfilingJob(ExecutionGraph executionGraph) {

		synchronized (this.registeredJobs) {
			this.registeredJobs.put(executionGraph.getJobID(), new JobProfilingData(executionGraph));
		}

	}


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
		if (this.profilingServer != null) {
			LOG.debug("Stopping profiling RPC server");
			this.profilingServer.stop();
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
	public void reportProfilingData(ProfilingDataContainer profilingDataContainer) {

		final long timestamp = System.currentTimeMillis();

		// Process the received profiling data
		final Iterator<InternalProfilingData> dataIterator = profilingDataContainer.getIterator();
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
