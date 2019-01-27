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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SubmittedJobGraph} instances for JobManagers running in {@link HighAvailabilityMode#FILESYSTEM}.
 *
 * Job graphs are stored in the directory specified in {@link HighAvailabilityOptions#HA_FILESYSTEM_JOBGRAPHS_PATH}
 */
public class FileSystemSubmittedJobGraphStore implements SubmittedJobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemSubmittedJobGraphStore.class);

	/** Lock to synchronize with the {@link SubmittedJobGraphListener}. */
	private final Object cacheLock = new Object();

	/** The full configured base path including the namespace. */
	private final String basePath;

	private final FileSystem jobsFileSystem;

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	/**
	 * Submitted job graph store backed by JobGraphStore
	 *
	 * @param currentJobsPath path for current job graphs
	 * @throws Exception
	 */
	public FileSystemSubmittedJobGraphStore(String currentJobsPath) throws Exception {

		checkNotNull(currentJobsPath, "Current jobs path");

		// Ensure that the job graphs path exists
		Path jobsPath = new Path(currentJobsPath);
		this.jobsFileSystem = jobsPath.getFileSystem();
		if (!jobsFileSystem.exists(jobsPath)) {
			jobsFileSystem.mkdirs(jobsPath);
		}

		this.basePath = currentJobsPath;
	}

	@Override
	public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {
		synchronized (cacheLock) {
			if (!isRunning) {
				isRunning = true;
			}
		}
	}

	@Override
	public void stop() {
		synchronized (cacheLock) {
			if (isRunning) {
				isRunning = false;
			}
		}
	}

	@Override
	public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");
		String path = getPathForJob(jobGraph.getJobId());

		LOG.debug("Adding job graph {} to {}{}.", jobGraph.getJobId(), basePath, path);

		synchronized (cacheLock) {
			verifyIsRunning();

			Path jobGraphPath = new Path(basePath + path);
			try {
				FSDataOutputStream outputStream = jobsFileSystem.create(jobGraphPath, true);
				outputStream.write(InstantiationUtil.serializeObject(jobGraph));
				outputStream.close();
			} catch (Exception e) {
				throw new RuntimeException("Storing job graph " + jobGraph.getJobId() + " failed", e);
			}
		}

		LOG.info("Added job graph {} to {}{}.", jobGraph, basePath, path);
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		String path = getPathForJob(jobId);

		LOG.debug("Removing job graph {} from {}{}.", jobId, basePath, path);

		synchronized (cacheLock) {
			try {
				jobsFileSystem.delete(new Path(basePath + path), true);
			} catch (Exception e) {
				LOG.info("Removing job graph " + jobId + " failed.", e);
			}
		}

		LOG.info("Removed job graph {} from {}{}.", jobId, basePath, path);
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		Collection<JobID> jobIds;

		LOG.debug("Retrieving all stored job ids.");

		final FileStatus[] fileStats = jobsFileSystem.listStatus(new Path(basePath));

		if (fileStats == null) {
			return Collections.emptyList();
		} else {
			jobIds = new ArrayList<>(fileStats.length);
			for (FileStatus fileStat : fileStats) {
				String path = fileStat.getPath().getName();
				try {
					jobIds.add(jobIdfromPath(path));
				} catch (Exception exception) {
					LOG.warn("Could not parse job id from {}. This indicates a malformed path.", path, exception);
				}
			}
		}

		return jobIds;
	}

	@Override
	public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		String path = getPathForJob(jobId);

		LOG.debug("Recovering job graph {} from {}{}.", jobId, basePath, path);

		synchronized (cacheLock) {
			verifyIsRunning();

			Path jobGraphPath = new Path(basePath + path);
			try {
				if (!jobsFileSystem.exists(jobGraphPath)) {
					return null;
				}
			} catch (Exception e) {
				throw new Exception("Could not retrieve the submitted job graph state handle " +
					"for " + path + "from the submitted job graph store.", e);
			}

			FSDataInputStream inputStream = jobsFileSystem.open(jobGraphPath);
			SubmittedJobGraph jobGraph = InstantiationUtil.deserializeObject(inputStream, ClassLoader.getSystemClassLoader());
			inputStream.close();

			LOG.info("Recovered {}.", jobId);

			return jobGraph;
		}
	}

	/**
	 * Verifies that the state is running.
	 */
	private void verifyIsRunning() {
		checkState(isRunning, "Not running. Forgot to call start()?");
	}

	/**
	 * Returns the JobID as a String (with leading slash).
	 */
	public static String getPathForJob(JobID jobId) {
		checkNotNull(jobId, "Job ID");
		return String.format("/%s", jobId);
	}

	/**
	 * Returns the JobID from the given path.
	 *
	 * @return JobID associated with the given path
	 */
	public static JobID jobIdfromPath(final String path) {
		return JobID.fromHexString(path);
	}
}
