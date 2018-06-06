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
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.FlinkException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SubmittedJobGraph} instances for JobManagers running in {@link HighAvailabilityMode#ZOOKEEPER}.
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
 * multiple instances operate concurrently. The job manager acts as a {@link SubmittedJobGraphListener}
 * to react to such situations.
 */
public class ZooKeeperSubmittedJobGraphStore implements SubmittedJobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSubmittedJobGraphStore.class);

	/** Lock to synchronize with the {@link SubmittedJobGraphListener}. */
	private final Object cacheLock = new Object();

	/** Client (not a namespace facade) */
	private final CuratorFramework client;

	/** The set of IDs of all added job graphs. */
	private final Set<JobID> addedJobGraphs = new HashSet<>();

	/** Completed checkpoints in ZooKeeper */
	private final ZooKeeperStateHandleStore<SubmittedJobGraph> jobGraphsInZooKeeper;

	/**
	 * Cache to monitor all children. This is used to detect races with other instances working
	 * on the same state.
	 */
	private final PathChildrenCache pathCache;

	/** The full configured base path including the namespace. */
	private final String zooKeeperFullBasePath;

	/** The external listener to be notified on races. */
	private SubmittedJobGraphListener jobGraphListener;

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	/**
	 * Submitted job graph store backed by ZooKeeper
	 *
	 * @param client ZooKeeper client
	 * @param currentJobsPath ZooKeeper path for current job graphs
	 * @param stateStorage State storage used to persist the submitted jobs
	 * @param executor to give to the ZooKeeperStateHandleStore to run ZooKeeper callbacks
	 * @throws Exception
	 */
	public ZooKeeperSubmittedJobGraphStore(
			CuratorFramework client,
			String currentJobsPath,
			RetrievableStateStorageHelper<SubmittedJobGraph> stateStorage,
			Executor executor) throws Exception {

		checkNotNull(currentJobsPath, "Current jobs path");
		checkNotNull(stateStorage, "State storage");

		// Keep a reference to the original client and not the namespace facade. The namespace
		// facade cannot be closed.
		this.client = checkNotNull(client, "Curator client");

		// Ensure that the job graphs path exists
		client.newNamespaceAwareEnsurePath(currentJobsPath)
				.ensure(client.getZookeeperClient());

		// All operations will have the path as root
		CuratorFramework facade = client.usingNamespace(client.getNamespace() + currentJobsPath);

		this.zooKeeperFullBasePath = client.getNamespace() + currentJobsPath;
		this.jobGraphsInZooKeeper = new ZooKeeperStateHandleStore<>(facade, stateStorage, executor);

		this.pathCache = new PathChildrenCache(facade, "/", false);
		pathCache.getListenable().addListener(new SubmittedJobGraphsPathCacheListener());
	}

	@Override
	public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {
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
					pathCache.close();
				} catch (Exception e) {
					throw new Exception("Could not properly stop the ZooKeeperSubmittedJobGraphStore.", e);
				} finally {
					isRunning = false;
				}
			}
		}
	}

	@Override
	@Nullable
	public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String path = getPathForJob(jobId);

		LOG.debug("Recovering job graph {} from {}{}.", jobId, zooKeeperFullBasePath, path);

		synchronized (cacheLock) {
			verifyIsRunning();

			boolean success = false;

			try {
				RetrievableStateHandle<SubmittedJobGraph> jobGraphRetrievableStateHandle;

				try {
					jobGraphRetrievableStateHandle = jobGraphsInZooKeeper.getAndLock(path);
				} catch (KeeperException.NoNodeException ignored) {
					success = true;
					return null;
				} catch (Exception e) {
					throw new FlinkException("Could not retrieve the submitted job graph state handle " +
						"for " + path + " from the submitted job graph store.", e);
				}
				SubmittedJobGraph jobGraph;

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

				addedJobGraphs.add(jobGraph.getJobId());

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
	public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");
		String path = getPathForJob(jobGraph.getJobId());

		LOG.debug("Adding job graph {} to {}{}.", jobGraph.getJobId(), zooKeeperFullBasePath, path);

		boolean success = false;

		while (!success) {
			synchronized (cacheLock) {
				verifyIsRunning();

				int currentVersion = jobGraphsInZooKeeper.exists(path);

				if (currentVersion == -1) {
					try {
						jobGraphsInZooKeeper.addAndLock(path, jobGraph);

						addedJobGraphs.add(jobGraph.getJobId());

						success = true;
					}
					catch (KeeperException.NodeExistsException ignored) {
					}
				}
				else if (addedJobGraphs.contains(jobGraph.getJobId())) {
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
				jobGraphsInZooKeeper.releaseAndTryRemove(path);

				addedJobGraphs.remove(jobId);
			}
		}

		LOG.info("Removed job graph {} from ZooKeeper.", jobId);
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
	private final class SubmittedJobGraphsPathCacheListener implements PathChildrenCacheListener {

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
							LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
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
							LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
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
					LOG.info("SubmittedJobGraphsPathCacheListener initialized");
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
