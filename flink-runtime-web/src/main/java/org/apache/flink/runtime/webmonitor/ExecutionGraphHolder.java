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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Gateway to obtaining an {@link ExecutionGraph} from a source, like JobManager or Archive.
 * <p>
 * The holder will cache the ExecutionGraph behind a LoadingCache, which will expire after 30
 * seconds since written.
 * Note that while the holder runs in the same JVM as the JobManager or Archive, the reference should
 * stay valid.
 */
public class ExecutionGraphHolder {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphHolder.class);

	private final FiniteDuration timeout;

	private AtomicReference<ActorGateway> jobManagerRef = new AtomicReference<>(null);

	private final LoadingCache<JobID, AccessExecutionGraph> cache =
		CacheBuilder.newBuilder()
			.maximumSize(1000)
			.expireAfterWrite(30, TimeUnit.SECONDS)
			.build(new CacheLoader<JobID, AccessExecutionGraph>() {
				@Override
				public AccessExecutionGraph load(JobID jobID) throws Exception {
					if (jobManagerRef.get() != null) {
						Future<Object> future = jobManagerRef.get().ask(new JobManagerMessages.RequestJob(jobID), timeout);
						Object result = Await.result(future, timeout);

						if (result instanceof JobManagerMessages.JobNotFound) {
							return null;
						}
						else if (result instanceof JobManagerMessages.JobFound) {
							AccessExecutionGraph eg = ((JobManagerMessages.JobFound) result).executionGraph();
							return eg;
						}
						else {
							throw new RuntimeException("Unknown response from JobManager / Archive: " + result);
						}
					}
					else {
						throw new RuntimeException("No connection to the leading JobManager.");
					}
				}
			});

	public ExecutionGraphHolder() {
		this(WebRuntimeMonitor.DEFAULT_REQUEST_TIMEOUT);
	}

	public ExecutionGraphHolder(FiniteDuration timeout) {
		this.timeout = checkNotNull(timeout);
	}

	/**
	 * Retrieves the execution graph with {@link JobID} jid or null if it cannot be found.
	 *
	 * @param jid jobID of the execution graph to be retrieved
	 * @return the retrieved execution graph or null if it is not retrievable
	 */
	public AccessExecutionGraph getExecutionGraph(JobID jid, ActorGateway jobManager) {
		if (jobManagerRef.get() == null) {
			this.jobManagerRef.set(jobManager);
		}
		try {
			return cache.get(jid);
		}
		catch (Exception e) {
			throw new RuntimeException("Error requesting execution graph", e);
		}
	}
}
