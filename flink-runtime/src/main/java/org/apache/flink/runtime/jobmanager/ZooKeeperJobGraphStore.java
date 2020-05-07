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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.flink.shaded.curator4.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link JobGraph} instances for JobManagers running in {@link HighAvailabilityMode#ZOOKEEPER}.
 *
 * <p>Each job graph creates ZNode:
 * <pre>
 * +----O /flink/jobgraphs/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/jobgraphs/&lt;job-id&gt; N [persistent]
 * </pre>
 *
 * <p>The root path is watched to detect concurrent modifications in corner situations where
 * multiple instances operate concurrently. The job manager acts as a {@link JobGraphListener}
 * to react to such situations.
 */
public class ZooKeeperJobGraphStore implements JobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperJobGraphStore.class);

	/** Lock to synchronize with the {@link JobGraphListener}. */
	private final Object cacheLock = new Object();

	/** The set of IDs of all added job graphs. */
	private final Set<JobID> addedJobGraphs = new HashSet<>();

	/** Submitted job graphs in ZooKeeper. */
	private final ZooKeeperStateHandleStore<JobGraph> jobGraphsInZooKeeper;

	/**
	 * Cache to monitor all children. This is used to detect races with other instances working
	 * on the same state.
	 */
	private final PathChildrenCache pathCache;

	/** The full configured base path including the namespace. */
	private final String zooKeeperFullBasePath;

	/** The external listener to be notified on races. */
	private JobGraphListener jobGraphListener;

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	/**
	 * Submitted job graph store backed by ZooKeeper.
	 *
	 * @param zooKeeperFullBasePath ZooKeeper path for current job graphs
	 * @param zooKeeperStateHandleStore State storage used to persist the submitted jobs
	 */
	public ZooKeeperJobGraphStore(
			String zooKeeperFullBasePath,
			ZooKeeperStateHandleStore<JobGraph> zooKeeperStateHandleStore,
			PathChildrenCache pathCache) {

		checkNotNull(zooKeeperFullBasePath, "Current jobs path");

		this.zooKeeperFullBasePath = zooKeeperFullBasePath;
		this.jobGraphsInZooKeeper = checkNotNull(zooKeeperStateHandleStore);

		this.pathCache = checkNotNull(pathCache);
		pathCache.getListenable().addListener(new JobGraphsPathCacheListener());
	}

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		synchronized (cacheLock) {
			if (!isRunning) {
				this.jobGraphListener = jobGraphListener;

				pathCache.start();

				isRunning = true;
			}
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (cacheLock) {
			if (isRunning) {
				jobGraphListener = null;

				try {
					Exception exception = null;

					try {
						jobGraphsInZooKeeper.releaseAll();
					} catch (Exception e) {
						exception = e;
					}

					try {
						pathCache.close();
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(e, exception);
					}

					if (exception != null) {
						throw new FlinkException("Could not properly stop the ZooKeeperJobGraphStore.", exception);
					}
				} finally {
					isRunning = false;
				}
			}
		}
	}

	@Override
	@Nullable
	public JobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String path = getPathForJob(jobId);

		LOG.debug("Recovering job graph {} from {}{}.", jobId, zooKeeperFullBasePath, path);

		synchronized (cacheLock) {
			verifyIsRunning();

			boolean success = false;

			try {
				RetrievableStateHandle<JobGraph> jobGraphRetrievableStateHandle;

				try {
					jobGraphRetrievableStateHandle = jobGraphsInZooKeeper.getAndLock(path);
				} catch (KeeperException.NoNodeException ignored) {
					success = true;
					return null;
				} catch (Exception e) {
					throw new FlinkException("Could not retrieve the submitted job graph state handle " +
						"for " + path + " from the submitted job graph store.", e);
				}
				JobGraph jobGraph;

				try {
					jobGraph = jobGraphRetrievableStateHandle.retrieveState();
				} catch (ClassNotFoundException cnfe) {
					throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " + path +
						". This indicates that you are trying to recover from state written by an " +
						"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
				} catch (IOException ioe) {
					throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " + path +
						". This indicates that the retrieved state handle is broken. Try cleaning the state handle " +
						"store.", ioe);
				}

				addedJobGraphs.add(jobGraph.getJobID());

				LOG.info("Recovered {}.", jobGraph);

				success = true;
				return jobGraph;
			} finally {
				if (!success) {
					jobGraphsInZooKeeper.release(path);
				}
			}
		}
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");
		String path = getPathForJob(jobGraph.getJobID());

		LOG.debug("Adding job graph {} to {}{}.", jobGraph.getJobID(), zooKeeperFullBasePath, path);

		boolean success = false;

		while (!success) {
			synchronized (cacheLock) {
				verifyIsRunning();

				int currentVersion = jobGraphsInZooKeeper.exists(path);

				if (currentVersion == -1) {
					try {
						jobGraphsInZooKeeper.addAndLock(path, jobGraph);

						addedJobGraphs.add(jobGraph.getJobID());

						success = true;
					}
					catch (KeeperException.NodeExistsException ignored) {
					}
				}
				else if (addedJobGraphs.contains(jobGraph.getJobID())) {
					try {
						jobGraphsInZooKeeper.replace(path, currentVersion, jobGraph);
						LOG.info("Updated {} in ZooKeeper.", jobGraph);

						success = true;
					}
					catch (KeeperException.NoNodeException ignored) {
					}
				}
				else {
					throw new IllegalStateException("Oh, no. Trying to update a graph you didn't " +
							"#getAllSubmittedJobGraphs() or #putJobGraph() yourself before.");
				}
			}
		}

		LOG.info("Added {} to ZooKeeper.", jobGraph);
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		String path = getPathForJob(jobId);

		LOG.debug("Removing job graph {} from {}{}.", jobId, zooKeeperFullBasePath, path);

		synchronized (cacheLock) {
			if (addedJobGraphs.contains(jobId)) {
				if (jobGraphsInZooKeeper.releaseAndTryRemove(path)) {
					addedJobGraphs.remove(jobId);
				} else {
					throw new FlinkException(String.format("Could not remove job graph with job id %s from ZooKeeper.", jobId));
				}
			}
		}

		LOG.info("Removed job graph {} from ZooKeeper.", jobId);
	}

	@Override
	public void releaseJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String path = getPathForJob(jobId);

		LOG.debug("Releasing locks of job graph {} from {}{}.", jobId, zooKeeperFullBasePath, path);

		synchronized (cacheLock) {
			if (addedJobGraphs.contains(jobId)) {
				jobGraphsInZooKeeper.release(path);

				addedJobGraphs.remove(jobId);
			}
		}

		LOG.info("Released locks of job graph {} from ZooKeeper.", jobId);
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		Collection<String> paths;

		LOG.debug("Retrieving all stored job ids from ZooKeeper under {}.", zooKeeperFullBasePath);

		try {
			paths = jobGraphsInZooKeeper.getAllPaths();
		} catch (Exception e) {
			throw new Exception("Failed to retrieve entry paths from ZooKeeperStateHandleStore.", e);
		}

		List<JobID> jobIds = new ArrayList<>(paths.size());

		for (String path : paths) {
			try {
				jobIds.add(jobIdfromPath(path));
			} catch (Exception exception) {
				LOG.warn("Could not parse job id from {}. This indicates a malformed path.", path, exception);
			}
		}

		return jobIds;
	}

	/**
	 * Monitors ZooKeeper for changes.
	 *
	 * <p>Detects modifications from other job managers in corner situations. The event
	 * notifications fire for changes from this job manager as well.
	 */
	private final class JobGraphsPathCacheListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
				throws Exception {

			if (LOG.isDebugEnabled()) {
				if (event.getData() != null) {
					LOG.debug("Received {} event (path: {})", event.getType(), event.getData().getPath());
				}
				else {
					LOG.debug("Received {} event", event.getType());
				}
			}

			switch (event.getType()) {
				case CHILD_ADDED: {
					JobID jobId = fromEvent(event);

					LOG.debug("Received CHILD_ADDED event notification for job {}", jobId);

					synchronized (cacheLock) {
						try {
							if (jobGraphListener != null && !addedJobGraphs.contains(jobId)) {
								try {
									// Whoa! This has been added by someone else. Or we were fast
									// to remove it (false positive).
									jobGraphListener.onAddedJobGraph(jobId);
								} catch (Throwable t) {
									LOG.error("Error in callback", t);
								}
							}
						} catch (Exception e) {
							LOG.error("Error in JobGraphsPathCacheListener", e);
						}
					}
				}
				break;

				case CHILD_UPDATED: {
					// Nothing to do
				}
				break;

				case CHILD_REMOVED: {
					JobID jobId = fromEvent(event);

					LOG.debug("Received CHILD_REMOVED event notification for job {}", jobId);

					synchronized (cacheLock) {
						try {
							if (jobGraphListener != null && addedJobGraphs.contains(jobId)) {
								try {
									// Oh oh. Someone else removed one of our job graphs. Mean!
									jobGraphListener.onRemovedJobGraph(jobId);
								} catch (Throwable t) {
									LOG.error("Error in callback", t);
								}
							}

							break;
						} catch (Exception e) {
							LOG.error("Error in JobGraphsPathCacheListener", e);
						}
					}
				}
				break;

				case CONNECTION_SUSPENDED: {
					LOG.warn("ZooKeeper connection SUSPENDING. Changes to the submitted job " +
						"graphs are not monitored (temporarily).");
				}
				break;

				case CONNECTION_LOST: {
					LOG.warn("ZooKeeper connection LOST. Changes to the submitted job " +
						"graphs are not monitored (permanently).");
				}
				break;

				case CONNECTION_RECONNECTED: {
					LOG.info("ZooKeeper connection RECONNECTED. Changes to the submitted job " +
						"graphs are monitored again.");
				}
				break;

				case INITIALIZED: {
					LOG.info("JobGraphsPathCacheListener initialized");
				}
				break;
			}
		}

		/**
		 * Returns a JobID for the event's path.
		 */
		private JobID fromEvent(PathChildrenCacheEvent event) {
			return JobID.fromHexString(ZKPaths.getNodeFromPath(event.getData().getPath()));
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
	 * Returns the JobID from the given path in ZooKeeper.
	 *
	 * @param path in ZooKeeper
	 * @return JobID associated with the given path
	 */
	public static JobID jobIdfromPath(final String path) {
		return JobID.fromHexString(path);
	}
}
