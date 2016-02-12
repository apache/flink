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
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.StringUtils;

import java.util.Map;

/**
 * Request handler for the STOP request.
 */
public class JobStoppingHandler implements RequestHandler {

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		try {
			JobID jobid = new JobID(StringUtils.hexStringToByte(pathParams.get("jobid")));
			if (jobManager != null) {
				jobManager.tell(new JobManagerMessages.StopJob(jobid));
				return "{}";
			}
			else {
				throw new Exception("No connection to the leading JobManager.");
			}
		}
		catch (Exception e) {
			throw new Exception("Failed to stop the job with id: "  + pathParams.get("jobid") + e.getMessage(), e);
		}
	}
}
