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

package org.apache.flink.runtime.webmonitor.retriever;

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Retrieves and stores the JobManagerGateway for the current leading JobManager.
 */
public abstract class JobManagerRetriever implements LeaderRetrievalListener {

	private final Logger log = LoggerFactory.getLogger(getClass());

	// False if we have to create a new JobManagerGateway future when being notified
	// about a new leader address
	private final AtomicBoolean firstTimeUsage;

	private volatile CompletableFuture<JobManagerGateway> jobManagerGatewayFuture;

	public JobManagerRetriever() {
		firstTimeUsage = new AtomicBoolean(true);
		jobManagerGatewayFuture = new CompletableFuture<>();
	}

	/**
	 * Returns the currently known leading job manager gateway and its web monitor port.
	 */
	public Optional<JobManagerGateway> getJobManagerGatewayNow() throws Exception {
		if (jobManagerGatewayFuture != null) {
			CompletableFuture<JobManagerGateway> jobManagerGatewayFuture = this.jobManagerGatewayFuture;

			if (jobManagerGatewayFuture.isDone()) {
				return Optional.of(jobManagerGatewayFuture.get());
			} else {
				return Optional.empty();
			}
		} else {
			return Optional.empty();
		}
	}

	/**
	 * Returns the current JobManagerGateway future.
	 */
	public CompletableFuture<JobManagerGateway> getJobManagerGateway() throws Exception {
		return jobManagerGatewayFuture;
	}

	@Override
	public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
		if (leaderAddress != null && !leaderAddress.equals("")) {
			try {
				final CompletableFuture<JobManagerGateway> newJobManagerGatewayFuture;

				if (firstTimeUsage.compareAndSet(true, false)) {
					newJobManagerGatewayFuture = jobManagerGatewayFuture;
				} else {
					newJobManagerGatewayFuture = new CompletableFuture<>();
					jobManagerGatewayFuture = newJobManagerGatewayFuture;
				}

				log.info("New leader reachable under {}:{}.", leaderAddress, leaderSessionID);

				createJobManagerGateway(leaderAddress, leaderSessionID).whenComplete(
					(JobManagerGateway jobManagerGateway, Throwable throwable) -> {
						if (throwable != null) {
							newJobManagerGatewayFuture.completeExceptionally(new FlinkException("Could not retrieve" +
								"the current job manager gateway.", throwable));
						} else {
							newJobManagerGatewayFuture.complete(jobManagerGateway);
						}
					}
				);
			}
			catch (Exception e) {
				handleError(e);
			}
		}
	}

	@Override
	public void handleError(Exception exception) {
		log.error("Received error from LeaderRetrievalService.", exception);

		jobManagerGatewayFuture.completeExceptionally(exception);
	}

	/**
	 * Create a JobManagerGateway for the given leader address and leader id.
	 *
	 * @param leaderAddress to connect against
	 * @param leaderId the endpoint currently uses
	 * @return Future containing the resolved JobManagerGateway
	 * @throws Exception if the JobManagerGateway creation failed
	 */
	protected abstract CompletableFuture<JobManagerGateway> createJobManagerGateway(String leaderAddress, UUID leaderId) throws Exception;
}
