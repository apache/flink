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

package org.apache.flink.runtime.highavailability;

import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A zookeeper based registry for running jobs, highly available.
 */
public class ZookeeperRegistry implements RunningJobsRegistry {
	
	private static final String DEFAULT_HA_JOB_REGISTRY_PATH = "/running_job_registry/";

	/** The ZooKeeper client to use */
	private final CuratorFramework client;

	private final String runningJobPath;

	private static final String HA_JOB_REGISTRY_PATH = "high-availability.zookeeper.job.registry";

	public ZookeeperRegistry(final CuratorFramework client, final Configuration configuration) {
		this.client = client;
		runningJobPath = configuration.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT) + 
			configuration.getString(HA_JOB_REGISTRY_PATH, DEFAULT_HA_JOB_REGISTRY_PATH);
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			String zkPath = runningJobPath + jobID.toString();
			this.client.newNamespaceAwareEnsurePath(zkPath).ensure(client.getZookeeperClient());
			this.client.setData().forPath(zkPath);
		}
		catch (Exception e) {
			throw new IOException("Set running state to zk fail for job " + jobID.toString(), e);
		}
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			String zkPath = runningJobPath + jobID.toString();
			this.client.newNamespaceAwareEnsurePath(zkPath).ensure(client.getZookeeperClient());
			this.client.delete().forPath(zkPath);
		}
		catch (Exception e) {
			throw new IOException("Set finished state to zk fail for job " + jobID.toString(), e);
		}
	}

	@Override
	public boolean isJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			Stat stat = client.checkExists().forPath(runningJobPath + jobID.toString());
			if (stat != null) {
				return true;
			}
			return false;
		}
		catch (Exception e) {
			throw new IOException("Get running state from zk fail for job " + jobID.toString(), e);
		}
	}
}
