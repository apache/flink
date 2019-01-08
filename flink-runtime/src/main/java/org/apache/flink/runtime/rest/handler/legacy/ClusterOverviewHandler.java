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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Responder that returns the status of the Flink cluster, such as how many
 * TaskManagers are currently connected, and how many jobs are running.
 */
public class ClusterOverviewHandler extends AbstractJsonRequestHandler {

	private static final String version = EnvironmentInformation.getVersion();

	private static final String commitID = EnvironmentInformation.getRevisionInformation().commitId;

	private final Time timeout;

	public ClusterOverviewHandler(Executor executor, Time timeout) {
		super(executor);
		this.timeout = checkNotNull(timeout);
	}

	@Override
	public String[] getPaths() {
		return new String[]{ClusterOverviewHeaders.URL};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		// we need no parameters, get all requests
		try {
			if (jobManagerGateway != null) {
				CompletableFuture<ClusterOverview> overviewFuture = jobManagerGateway.requestClusterOverview(timeout);

				return overviewFuture.thenApplyAsync(
					(ClusterOverview overview) -> {
						StringWriter writer = new StringWriter();
						try {
							JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

							gen.writeStartObject();
							gen.writeNumberField(ClusterOverview.FIELD_NAME_TASKMANAGERS, overview.getNumTaskManagersConnected());
							gen.writeNumberField(ClusterOverview.FIELD_NAME_SLOTS_TOTAL, overview.getNumSlotsTotal());
							gen.writeNumberField(ClusterOverview.FIELD_NAME_SLOTS_AVAILABLE, overview.getNumSlotsAvailable());
							gen.writeNumberField(JobsOverview.FIELD_NAME_JOBS_RUNNING, overview.getNumJobsRunningOrPending());
							gen.writeNumberField(JobsOverview.FIELD_NAME_JOBS_FINISHED, overview.getNumJobsFinished());
							gen.writeNumberField(JobsOverview.FIELD_NAME_JOBS_CANCELLED, overview.getNumJobsCancelled());
							gen.writeNumberField(JobsOverview.FIELD_NAME_JOBS_FAILED, overview.getNumJobsFailed());
							gen.writeStringField(ClusterOverviewWithVersion.FIELD_NAME_VERSION, version);
							if (!commitID.equals(EnvironmentInformation.UNKNOWN)) {
								gen.writeStringField(ClusterOverviewWithVersion.FIELD_NAME_COMMIT, commitID);
							}
							gen.writeEndObject();

							gen.close();
							return writer.toString();
						} catch (IOException exception) {
							throw new CompletionException(new FlinkException("Could not write cluster overview.", exception));
						}
					},
					executor);
			} else {
				throw new Exception("No connection to the leading JobManager.");
			}
		}
		catch (Exception e) {
			return FutureUtils.completedExceptionally(new FlinkException("Failed to fetch list of all running jobs: ", e));
		}
	}
}
