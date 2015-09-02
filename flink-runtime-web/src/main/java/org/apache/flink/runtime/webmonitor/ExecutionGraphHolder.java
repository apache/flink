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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.WeakHashMap;

/**
 * Gateway to obtaining an {@link ExecutionGraph} from a source, like JobManager or Archive.
 * <p>
 * The holder will cache the ExecutionGraph behind a weak reference, which will be cleared
 * at some point once no one else is pointing to the ExecutionGraph.
 * Note that while the holder runs in the same JVM as the JobManager or Archive, the reference should
 * stay valid.
 */
public class ExecutionGraphHolder {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphHolder.class);

	/** Retrieves the current leading JobManager and its corresponding archive */
	private final JobManagerArchiveRetriever retriever;
	
	private final FiniteDuration timeout;
	
	private final WeakHashMap<JobID, ExecutionGraph> cache = new WeakHashMap<JobID, ExecutionGraph>();

	public ExecutionGraphHolder(JobManagerArchiveRetriever retriever) {
		this(retriever, WebRuntimeMonitor.DEFAULT_REQUEST_TIMEOUT);
	}

	public ExecutionGraphHolder(JobManagerArchiveRetriever retriever, FiniteDuration timeout) {
		if (retriever == null || timeout == null) {
			throw new NullPointerException();
		}
		this.retriever = retriever;
		this.timeout = timeout;
	}

	/**
	 * Retrieves the execution graph with {@link JobID} jid or null if it cannot be found.
	 *
	 * @param jid jobID of the execution graph to be retrieved
	 * @return the retrieved execution graph or null if it is not retrievable
	 */
	public ExecutionGraph getExecutionGraph(JobID jid) {
		ExecutionGraph cached = cache.get(jid);
		if (cached != null) {
			return cached;
		}
		
		try {
			ActorGateway jobManager = retriever.getJobManagerGateway();

			if (jobManager != null) {

				Future<Object> future = jobManager.ask(new JobManagerMessages.RequestJob(jid), timeout);
				Object result = Await.result(future, timeout);
				if (result instanceof JobManagerMessages.JobNotFound) {
					return null;
				} else if (result instanceof JobManagerMessages.JobFound) {
					ExecutionGraph eg = ((JobManagerMessages.JobFound) result).executionGraph();
					cache.put(jid, eg);
					return eg;
				} else {
					throw new RuntimeException("Unknown response from JobManager / Archive: " + result);
				}
			} else {
				LOG.warn("No connection to the leading JobManager.");
				return null;
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Error requesting execution graph", e);
		}
	}
}
