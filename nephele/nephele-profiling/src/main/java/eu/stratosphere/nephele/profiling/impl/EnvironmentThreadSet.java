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

import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.impl.types.InternalExecutionVertexThreadProfilingData;

public class EnvironmentThreadSet {

	private static final long NANO_TO_MILLISECONDS = 1000 * 1000;

	private static final long PERCENT = 100;

	private class CPUUtilizationSnapshot {

		private final long timestamp;

		private final long totalCPUTime;

		private final long totalCPUUserTime;

		private final long totalCPUWaitTime;

		private final long totalCPUBlockTime;

		public CPUUtilizationSnapshot(long timestamp, long totalCPUTime, long totalCPUUserTime, long totalCPUWaitTime,
				long totalCPUBlockTime) {
			this.timestamp = timestamp;
			this.totalCPUTime = totalCPUTime;
			this.totalCPUUserTime = totalCPUUserTime;
			this.totalCPUWaitTime = totalCPUWaitTime;
			this.totalCPUBlockTime = totalCPUBlockTime;
		}

		public long getTimestamp() {
			return this.timestamp;
		}

		public long getTotalCPUTime() {
			return this.totalCPUTime;
		}

		public long getTotalCPUUserTime() {
			return this.totalCPUUserTime;
		}

		public long getTotalCPUWaitTime() {
			return this.totalCPUWaitTime;
		}

		public long getTotalCPUBlockTime() {
			return this.totalCPUBlockTime;
		}
	}

	private final Thread mainThread;

	private final ExecutionVertexID executionVertexID;

	private final Map<Thread, CPUUtilizationSnapshot> userThreads = new HashMap<Thread, CPUUtilizationSnapshot>();

	private CPUUtilizationSnapshot mainThreadSnapshot = null;

	public EnvironmentThreadSet(ThreadMXBean tmx, Thread mainThread, ExecutionVertexID executionVertexID) {
		this.mainThread = mainThread;
		this.executionVertexID = executionVertexID;

		this.mainThreadSnapshot = createCPUUtilizationSnapshot(tmx, mainThread, System.currentTimeMillis());
	}

	public Thread getMainThread() {
		return this.mainThread;
	}

	public void addUserThread(ThreadMXBean tmx, Thread thread) {

		synchronized (this.userThreads) {
			this.userThreads.put(thread, createCPUUtilizationSnapshot(tmx, thread, System.currentTimeMillis()));
		}
	}

	public void removeUserThread(Thread thread) {

		synchronized (this.userThreads) {
			this.userThreads.remove(thread);
		}
	}

	public int getNumberOfUserThreads() {

		synchronized (this.userThreads) {
			return this.userThreads.size();
		}

	}

	private CPUUtilizationSnapshot createCPUUtilizationSnapshot(ThreadMXBean tmx, Thread thread, long timestamp) {

		final long threadId = thread.getId();

		return new CPUUtilizationSnapshot(timestamp, tmx.getThreadCpuTime(threadId) / NANO_TO_MILLISECONDS, tmx
			.getThreadUserTime(threadId)
			/ NANO_TO_MILLISECONDS, tmx.getThreadInfo(threadId).getWaitedTime(), tmx.getThreadInfo(threadId)
			.getBlockedTime());
	}

	public InternalExecutionVertexThreadProfilingData captureCPUUtilization(JobID jobID, ThreadMXBean tmx,
			long timestamp) {

		synchronized (this.userThreads) {

			// Calculate utilization for main thread first
			CPUUtilizationSnapshot newMainThreadSnapshot = createCPUUtilizationSnapshot(tmx, this.mainThread, timestamp);
			final long mainInterval = newMainThreadSnapshot.getTimestamp() - this.mainThreadSnapshot.getTimestamp();

			if (mainInterval == 0) {
				return null;
			}

			long cputime = newMainThreadSnapshot.getTotalCPUTime() - this.mainThreadSnapshot.getTotalCPUTime();
			long usrtime = newMainThreadSnapshot.getTotalCPUUserTime() - this.mainThreadSnapshot.getTotalCPUUserTime();
			long systime = cputime - usrtime;
			long waitime = newMainThreadSnapshot.getTotalCPUWaitTime() - this.mainThreadSnapshot.getTotalCPUWaitTime();
			long blktime = newMainThreadSnapshot.getTotalCPUBlockTime()
				- this.mainThreadSnapshot.getTotalCPUBlockTime();

			int sumUsrTime = (int) ((usrtime * PERCENT) / mainInterval);
			int sumSysTime = (int) ((systime * PERCENT) / mainInterval);
			int sumBlkTime = (int) ((blktime * PERCENT) / mainInterval);
			int sumWaiTime = (int) ((waitime * PERCENT) / mainInterval);

			// Update snapshot
			this.mainThreadSnapshot = newMainThreadSnapshot;

			if (!this.userThreads.isEmpty()) {

				final Iterator<Thread> it = this.userThreads.keySet().iterator();
				int divisor = this.userThreads.size();
				while (it.hasNext()) {

					final Thread userThread = it.next();
					final CPUUtilizationSnapshot newUtilizationSnaphot = createCPUUtilizationSnapshot(tmx, userThread,
						timestamp);
					final CPUUtilizationSnapshot oldUtilizationSnapshot = this.userThreads.get(userThread);

					long interval = newUtilizationSnaphot.getTimestamp() - oldUtilizationSnapshot.getTimestamp();

					if (interval == 0) {
						--divisor;
						continue;
					}

					cputime = newUtilizationSnaphot.getTotalCPUTime() - oldUtilizationSnapshot.getTotalCPUTime();
					usrtime = newUtilizationSnaphot.getTotalCPUUserTime()
						- oldUtilizationSnapshot.getTotalCPUUserTime();
					systime = cputime - usrtime;
					waitime = newUtilizationSnaphot.getTotalCPUWaitTime()
						- oldUtilizationSnapshot.getTotalCPUWaitTime();
					blktime = newUtilizationSnaphot.getTotalCPUBlockTime()
						- oldUtilizationSnapshot.getTotalCPUBlockTime();

					sumUsrTime += (int) ((usrtime * PERCENT) / interval);
					sumSysTime += (int) ((systime * PERCENT) / interval);
					sumBlkTime += (int) ((blktime * PERCENT) / interval);
					sumWaiTime += (int) ((waitime * PERCENT) / interval);

					// Update snapshot
					this.userThreads.put(userThread, newUtilizationSnaphot);
				}

				sumUsrTime /= (divisor + 1);
				sumSysTime /= (divisor + 1);
				sumBlkTime /= (divisor + 1);
				sumWaiTime /= (divisor + 1);
			}

			return new InternalExecutionVertexThreadProfilingData(jobID, this.executionVertexID, (int) mainInterval,
				sumUsrTime, sumSysTime, sumBlkTime, sumWaiTime);
		}
	}
}
