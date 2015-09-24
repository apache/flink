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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.NotFoundException;

import java.util.Map;

/**
 * Base class for request handlers whose response depends on an ExecutionGraph
 * that can be retrieved via "jobid" parameter.
 */
public abstract class AbstractExecutionGraphRequestHandler implements RequestHandler {
	
	private final ExecutionGraphHolder executionGraphHolder;
	
	
	public AbstractExecutionGraphRequestHandler(ExecutionGraphHolder executionGraphHolder) {
		this.executionGraphHolder = executionGraphHolder;
	}
	
	
	@Override
	public final String handleRequest(Map<String, String> params) throws Exception {
		String jidString = params.get("jobid");
		if (jidString == null) {
			throw new RuntimeException("JobId parameter missing");
		}

		JobID jid;
		try {
			jid = JobID.fromHexString(jidString);
		}
		catch (Exception e) {
			throw new RuntimeException("Invalid JobID string '" + jidString + "': " + e.getMessage()); 
		}
		
		ExecutionGraph eg = executionGraphHolder.getExecutionGraph(jid);
		if (eg == null) {
			throw new NotFoundException("Could not find job with id " + jid);
		}
		
		return handleRequest(eg, params);
	}
	
	public abstract String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception;
}
