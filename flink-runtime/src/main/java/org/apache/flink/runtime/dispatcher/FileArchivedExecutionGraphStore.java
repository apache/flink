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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.flink.shaded.guava18.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Store for {@link ArchivedExecutionGraph}. The store writes the archived execution graph to disk
 * and keeps the most recently used execution graphs in a memory cache for faster serving. Moreover,
 * the stored execution graphs are periodically cleaned up.
 */
public class FileArchivedExecutionGraphStore implements ArchivedExecutionGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileArchivedExecutionGraphStore.class);

	private final File storageDir;

	private final Cache<JobID, JobDetails> jobDetailsCache;

	private final LoadingCache<JobID, ArchivedExecutionGraph> archivedExecutionGraphCache;

	private final ScheduledFuture<?> cleanupFuture;

	private final Thread shutdownHook;

	private int numFinishedJobs;

	private int numFailedJobs;

	private int numCanceledJobs;

	public FileArchivedExecutionGraphStore(
			File rootDir,
			Time expirationTime,
			int maximumCapacity,
			long maximumCacheSizeBytes,
			ScheduledExecutor scheduledExecutor,
			Ticker ticker) throws IOException {

		final File storageDirectory = initExecutionGraphStorageDirectory(rootDir);

		LOG.info(
			"Initializing {}: Storage directory {}, expiration time {}, maximum cache size {} bytes.",
			FileArchivedExecutionGraphStore.class.getSimpleName(),
			storageDirectory,
			expirationTime.toMilliseconds(),
			maximumCacheSizeBytes);

		this.storageDir = Preconditions.checkNotNull(storageDirectory);
		Preconditions.checkArgument(
			storageDirectory.exists() && storageDirectory.isDirectory(),
			"The storage directory must exist and be a directory.");
		this.jobDetailsCache = CacheBuilder.newBuilder()
			.expireAfterWrite(expirationTime.toMilliseconds(), TimeUnit.MILLISECONDS)
			.maximumSize(maximumCapacity)
			.removalListener(
				(RemovalListener<JobID, JobDetails>) notification -> deleteExecutionGraphFile(notification.getKey()))
			.ticker(ticker)
			.build();

		this.archivedExecutionGraphCache = CacheBuilder.newBuilder()
			.maximumWeight(maximumCacheSizeBytes)
			.weigher(this::calculateSize)
			.build(new CacheLoader<JobID, ArchivedExecutionGraph>() {
				@Override
				public ArchivedExecutionGraph load(JobID jobId) throws Exception {
					return loadExecutionGraph(jobId);
				}});

		this.cleanupFuture = scheduledExecutor.scheduleWithFixedDelay(
			jobDetailsCache::cleanUp,
			expirationTime.toMilliseconds(),
			expirationTime.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		this.shutdownHook = ShutdownHookUtil.addShutdownHook(this, getClass().getSimpleName(), LOG);

		this.numFinishedJobs = 0;
		this.numFailedJobs = 0;
		this.numCanceledJobs = 0;
	}

	@Override
	public int size() {
		return Math.toIntExact(jobDetailsCache.size());
	}

	@Override
	@Nullable
	public ArchivedExecutionGraph get(JobID jobId) {
		try {
			return archivedExecutionGraphCache.get(jobId);
		} catch (ExecutionException e) {
			LOG.debug("Could not load archived execution graph for job id {}.", jobId, e);
			return null;
		}
	}

	@Override
	public void put(ArchivedExecutionGraph archivedExecutionGraph) throws IOException {
		final JobStatus jobStatus = archivedExecutionGraph.getState();
		final JobID jobId = archivedExecutionGraph.getJobID();
		final String jobName = archivedExecutionGraph.getJobName();

		Preconditions.checkArgument(
			jobStatus.isGloballyTerminalState(),
			"The job " + jobName + '(' + jobId +
				") is not in a globally terminal state. Instead it is in state " + jobStatus + '.');

		switch (jobStatus) {
			case FINISHED:
				numFinishedJobs++;
				break;
			case CANCELED:
				numCanceledJobs++;
				break;
			case FAILED:
				numFailedJobs++;
				break;
			default:
				throw new IllegalStateException("The job " + jobName + '(' +
					jobId + ") should have been in a globally terminal state. " +
					"Instead it was in state " + jobStatus + '.');
		}

		// write the ArchivedExecutionGraph to disk
		storeArchivedExecutionGraph(archivedExecutionGraph);

		final JobDetails detailsForJob = WebMonitorUtils.createDetailsForJob(archivedExecutionGraph);

		jobDetailsCache.put(jobId, detailsForJob);
		archivedExecutionGraphCache.put(jobId, archivedExecutionGraph);
	}

	@Override
	public JobsOverview getStoredJobsOverview() {
		return new JobsOverview(0, numFinishedJobs, numCanceledJobs, numFailedJobs);
	}

	@Override
	public Collection<JobDetails> getAvailableJobDetails() {
		return jobDetailsCache.asMap().values();
	}

	@Nullable
	@Override
	public JobDetails getAvailableJobDetails(JobID jobId) {
		return jobDetailsCache.getIfPresent(jobId);
	}

	@Override
	public void close() throws IOException {
		cleanupFuture.cancel(false);

		jobDetailsCache.invalidateAll();

		// clean up the storage directory
		FileUtils.deleteFileOrDirectory(storageDir);

		// Remove shutdown hook to prevent resource leaks
		ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
	}

	// --------------------------------------------------------------
	// Internal methods
	// --------------------------------------------------------------

	private int calculateSize(JobID jobId, ArchivedExecutionGraph serializableExecutionGraph) {
		final File archivedExecutionGraphFile = getExecutionGraphFile(jobId);

		if (archivedExecutionGraphFile.exists()) {
			return Math.toIntExact(archivedExecutionGraphFile.length());
		} else {
			LOG.debug("Could not find archived execution graph file for {}. Estimating the size instead.", jobId);
			return serializableExecutionGraph.getAllVertices().size() * 1000 +
				serializableExecutionGraph.getAccumulatorsSerialized().size() * 1000;
		}
	}

	private ArchivedExecutionGraph loadExecutionGraph(JobID jobId) throws IOException, ClassNotFoundException {
		final File archivedExecutionGraphFile = getExecutionGraphFile(jobId);

		if (archivedExecutionGraphFile.exists()) {
			try (FileInputStream fileInputStream = new FileInputStream(archivedExecutionGraphFile)) {
				return InstantiationUtil.deserializeObject(fileInputStream, getClass().getClassLoader());
			}
		} else {
			throw new FileNotFoundException("Could not find file for archived execution graph " + jobId +
				". This indicates that the file either has been deleted or never written.");
		}
	}

	private void storeArchivedExecutionGraph(ArchivedExecutionGraph archivedExecutionGraph) throws IOException {
		final File archivedExecutionGraphFile = getExecutionGraphFile(archivedExecutionGraph.getJobID());

		try (FileOutputStream fileOutputStream = new FileOutputStream(archivedExecutionGraphFile)) {
			InstantiationUtil.serializeObject(fileOutputStream, archivedExecutionGraph);
		}
	}

	private File getExecutionGraphFile(JobID jobId) {
		return new File(storageDir, jobId.toString());
	}

	private void deleteExecutionGraphFile(JobID jobId) {
		Preconditions.checkNotNull(jobId);

		final File archivedExecutionGraphFile = getExecutionGraphFile(jobId);

		try {
			FileUtils.deleteFileOrDirectory(archivedExecutionGraphFile);
		} catch (IOException e) {
			LOG.debug("Could not delete file {}.", archivedExecutionGraphFile, e);
		}

		archivedExecutionGraphCache.invalidate(jobId);
		jobDetailsCache.invalidate(jobId);
	}

	private static File initExecutionGraphStorageDirectory(File tmpDir) throws IOException {
		final int maxAttempts = 10;

		for (int attempt = 0; attempt < maxAttempts; attempt++) {
			final File storageDirectory = new File(tmpDir, "executionGraphStore-" + UUID.randomUUID());

			if (storageDirectory.mkdir()) {
				return storageDirectory;
			}
		}

		throw new IOException("Could not create executionGraphStorage directory in " + tmpDir + '.');
	}

	// --------------------------------------------------------------
	// Testing methods
	// --------------------------------------------------------------

	@VisibleForTesting
	File getStorageDir() {
		return storageDir;
	}

	@VisibleForTesting
	LoadingCache<JobID, ArchivedExecutionGraph> getArchivedExecutionGraphCache() {
		return archivedExecutionGraphCache;
	}
}
