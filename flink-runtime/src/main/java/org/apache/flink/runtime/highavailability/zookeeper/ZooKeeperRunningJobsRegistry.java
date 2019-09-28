/*
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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A zookeeper based registry for running jobs, highly available.
 */
public class ZooKeeperRunningJobsRegistry implements RunningJobsRegistry {

	private static final Charset ENCODING = Charset.forName("utf-8");

	/** The ZooKeeper client to use. */
	private final CuratorFramework client;

	private final String runningJobPath;

	public ZooKeeperRunningJobsRegistry(final CuratorFramework client, final Configuration configuration) {
		this.client = checkNotNull(client, "client");
		this.runningJobPath = configuration.getString(HighAvailabilityOptions.ZOOKEEPER_RUNNING_JOB_REGISTRY_PATH);
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			writeEnumToZooKeeper(jobID, JobSchedulingStatus.RUNNING);
		}
		catch (Exception e) {
			throw new IOException("Failed to set RUNNING state in ZooKeeper for job " + jobID, e);
		}
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			writeEnumToZooKeeper(jobID, JobSchedulingStatus.DONE);
		}
		catch (Exception e) {
			throw new IOException("Failed to set DONE state in ZooKeeper for job " + jobID, e);
		}
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			final String zkPath = createZkPath(jobID);
			final Stat stat = client.checkExists().forPath(zkPath);
			if (stat != null) {
				// found some data, try to parse it
				final byte[] data = client.getData().forPath(zkPath);
				if (data != null) {
					try {
						final String name = new String(data, ENCODING);
						return JobSchedulingStatus.valueOf(name);
					}
					catch (IllegalArgumentException e) {
						throw new IOException("Found corrupt data in ZooKeeper: " +
								Arrays.toString(data) + " is no valid job status");
					}
				}
			}

			// nothing found, yet, must be in status 'PENDING'
			return JobSchedulingStatus.PENDING;
		}
		catch (Exception e) {
			throw new IOException("Get finished state from zk fail for job " + jobID.toString(), e);
		}
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			final String zkPath = createZkPath(jobID);
			this.client.newNamespaceAwareEnsurePath(zkPath).ensure(client.getZookeeperClient());
			this.client.delete().forPath(zkPath);
		}
		catch (Exception e) {
			throw new IOException("Failed to clear job state from ZooKeeper for job " + jobID, e);
		}
	}

	private String createZkPath(JobID jobID) {
		return runningJobPath + jobID.toString();
	}

	private void writeEnumToZooKeeper(JobID jobID, JobSchedulingStatus status) throws Exception {
		final String zkPath = createZkPath(jobID);
		this.client.newNamespaceAwareEnsurePath(zkPath).ensure(client.getZookeeperClient());
		this.client.setData().forPath(zkPath, status.name().getBytes(ENCODING));
	}
}
