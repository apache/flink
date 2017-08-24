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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Map;

/**
 * Request handler for the CANCEL request.
 */
public class JobCancellationHandler extends AbstractJsonRequestHandler {

	private static final String JOB_CONCELLATION_REST_PATH = "/jobs/:jobid/cancel";
	private static final String JOB_CONCELLATION_YARN_REST_PATH = "/jobs/:jobid/yarn-cancel";

	private final Time timeout;

	public JobCancellationHandler(Time timeout) {
		this.timeout = Preconditions.checkNotNull(timeout);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_CONCELLATION_REST_PATH, JOB_CONCELLATION_YARN_REST_PATH};
	}

	@Override
	public String handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) throws Exception {
		try {
			JobID jobId = new JobID(StringUtils.hexStringToByte(pathParams.get("jobid")));
			if (jobManagerGateway != null) {
				jobManagerGateway.cancelJob(jobId, timeout);
				return "{}";
			}
			else {
				throw new Exception("No connection to the leading JobManager.");
			}
		}
		catch (Exception e) {
			throw new Exception("Failed to cancel the job with id: "  + pathParams.get("jobid") + e.getMessage(), e);
		}
	}
}
