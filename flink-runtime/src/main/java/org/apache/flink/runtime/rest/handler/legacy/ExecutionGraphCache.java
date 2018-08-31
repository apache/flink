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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Cache for {@link AccessExecutionGraph} which are obtained from the Flink cluster. Every cache entry
 * has an associated time to live after which a new request will trigger the reloading of the
 * {@link AccessExecutionGraph} from the cluster.
 */
public class ExecutionGraphCache implements Closeable {

	private final Time timeout;

	private final Time timeToLive;

	private final ConcurrentHashMap<JobID, ExecutionGraphEntry> cachedExecutionGraphs;

	private volatile boolean running = true;

	public ExecutionGraphCache(
			Time timeout,
			Time timeToLive) {
		this.timeout = checkNotNull(timeout);
		this.timeToLive = checkNotNull(timeToLive);

		cachedExecutionGraphs = new ConcurrentHashMap<>(4);
	}

	@Override
	public void close() {
		running = false;

		// clear all cached AccessExecutionGraphs
		cachedExecutionGraphs.clear();
	}

	/**
	 * Gets the number of cache entries.
	 */
	public int size() {
		return cachedExecutionGraphs.size();
	}

	/**
	 * Gets the {@link AccessExecutionGraph} for the given {@link JobID} and caches it. The
	 * {@link AccessExecutionGraph} will be requested again after the refresh interval has passed
	 * or if the graph could not be retrieved from the given gateway.
	 *
	 * @param jobId identifying the {@link AccessExecutionGraph} to get
	 * @param restfulGateway to request the {@link AccessExecutionGraph} from
	 * @return Future containing the requested {@link AccessExecutionGraph}
	 */
	public CompletableFuture<AccessExecutionGraph> getExecutionGraph(JobID jobId, RestfulGateway restfulGateway) {

		Preconditions.checkState(running, "ExecutionGraphCache is no longer running");

		while (true) {
			final ExecutionGraphEntry oldEntry = cachedExecutionGraphs.get(jobId);

			final long currentTime = System.currentTimeMillis();

			if (oldEntry != null) {
				if (currentTime < oldEntry.getTTL()) {
					final CompletableFuture<AccessExecutionGraph> executionGraphFuture = oldEntry.getExecutionGraphFuture();
					if (executionGraphFuture.isDone() && !executionGraphFuture.isCompletedExceptionally()) {

						// TODO: Remove once we no longer request the actual ExecutionGraph from the JobManager but only the ArchivedExecutionGraph
						try {
							final AccessExecutionGraph executionGraph = executionGraphFuture.get();
							if (executionGraph.getState() != JobStatus.SUSPENDING &&
								executionGraph.getState() != JobStatus.SUSPENDED) {
								return executionGraphFuture;
							}
							// send a new request to get the ExecutionGraph from the new leader
						} catch (InterruptedException | ExecutionException e) {
							throw new RuntimeException("Could not retrieve ExecutionGraph from the orderly completed future. This should never happen.", e);
						}
					} else if (!executionGraphFuture.isDone()) {
						return executionGraphFuture;
					}
					// otherwise it must be completed exceptionally
				}
			}

			final ExecutionGraphEntry newEntry = new ExecutionGraphEntry(currentTime + timeToLive.toMilliseconds());

			final boolean successfulUpdate;

			if (oldEntry == null) {
				successfulUpdate = cachedExecutionGraphs.putIfAbsent(jobId, newEntry) == null;
			} else {
				successfulUpdate = cachedExecutionGraphs.replace(jobId, oldEntry, newEntry);
				// cancel potentially outstanding futures
				oldEntry.getExecutionGraphFuture().cancel(false);
			}

			if (successfulUpdate) {
				final CompletableFuture<? extends AccessExecutionGraph> executionGraphFuture = restfulGateway.requestJob(jobId, timeout);

				executionGraphFuture.whenComplete(
					(AccessExecutionGraph executionGraph, Throwable throwable) -> {
						if (throwable != null) {
							newEntry.getExecutionGraphFuture().completeExceptionally(throwable);

							// remove exceptionally completed entry because it doesn't help
							cachedExecutionGraphs.remove(jobId, newEntry);
						} else {
							newEntry.getExecutionGraphFuture().complete(executionGraph);

							// TODO: Remove once we no longer request the actual ExecutionGraph from the JobManager but only the ArchivedExecutionGraph
							if (executionGraph.getState() == JobStatus.SUSPENDING ||
								executionGraph.getState() == JobStatus.SUSPENDED) {
								// remove the entry in case of suspension --> triggers new request when accessed next time
								cachedExecutionGraphs.remove(jobId, newEntry);
							}
						}
					});

				if (!running) {
					// delete newly added entry in case of a concurrent stopping operation
					cachedExecutionGraphs.remove(jobId, newEntry);
				}

				return newEntry.getExecutionGraphFuture();
			}
		}
	}

	/**
	 * Perform the cleanup of out dated {@link ExecutionGraphEntry}.
	 */
	public void cleanup() {
		long currentTime = System.currentTimeMillis();

		// remove entries which have exceeded their time to live
		cachedExecutionGraphs.values().removeIf(
			(ExecutionGraphEntry entry) -> currentTime >= entry.getTTL());
	}

	/**
	 * Wrapper containing the current execution graph and it's time to live (TTL).
	 */
	private static final class ExecutionGraphEntry {
		private final long ttl;

		private final CompletableFuture<AccessExecutionGraph> executionGraphFuture;

		ExecutionGraphEntry(long ttl) {
			this.ttl = ttl;
			this.executionGraphFuture = new CompletableFuture<>();
		}

		public long getTTL() {
			return ttl;
		}

		public CompletableFuture<AccessExecutionGraph> getExecutionGraphFuture() {
			return executionGraphFuture;
		}
	}
}
