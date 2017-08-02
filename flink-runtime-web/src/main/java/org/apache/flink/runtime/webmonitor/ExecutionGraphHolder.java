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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Gateway to obtaining an {@link ExecutionGraph} from a source, like JobManager or Archive.
 *
 * <p>The holder will cache the ExecutionGraph behind a weak reference, which will be cleared
 * at some point once no one else is pointing to the ExecutionGraph.
 * Note that while the holder runs in the same JVM as the JobManager or Archive, the reference should
 * stay valid.
 */
public class ExecutionGraphHolder {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphHolder.class);

	private final Time timeout;

	private final WeakHashMap<JobID, AccessExecutionGraph> cache = new WeakHashMap<>();

	public ExecutionGraphHolder() {
		this(WebRuntimeMonitor.DEFAULT_REQUEST_TIMEOUT);
	}

	public ExecutionGraphHolder(Time timeout) {
		this.timeout = checkNotNull(timeout);
	}

	/**
	 * Retrieves the execution graph with {@link JobID} jid wrapped in {@link Optional} or
	 * {@link Optional#empty()} if it cannot be found.
	 *
	 * @param jid jobID of the execution graph to be retrieved
	 * @return Optional ExecutionGraph if it has been retrievable, empty if there has been no ExecutionGraph
	 * @throws Exception if the ExecutionGraph retrieval failed.
	 */
	public Optional<AccessExecutionGraph> getExecutionGraph(JobID jid, JobManagerGateway jobManagerGateway) throws Exception {
		AccessExecutionGraph cached = cache.get(jid);
		if (cached != null) {
			if (cached.getState() == JobStatus.SUSPENDED) {
				cache.remove(jid);
			} else {
				return Optional.of(cached);
			}
		}

		CompletableFuture<Optional<AccessExecutionGraph>> executionGraphFuture = jobManagerGateway.requestJob(jid, timeout);

		Optional<AccessExecutionGraph> result = executionGraphFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

		return result.map((executionGraph) -> {
			cache.put(jid, executionGraph);

			return executionGraph;
		});
	}
}
