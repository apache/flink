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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.JsonFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Request handler that returns the configuration of a job.
 */
public class JobExceptionsHandler extends AbstractExecutionGraphRequestHandler implements RequestHandler.JsonResponse {

	private static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;
	
	public JobExceptionsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {
		// most important is the root failure cause
		Throwable rootException = graph.getFailureCause();

		// we additionally collect all exceptions (up to a limit) that occurred in the individual tasks
		List<ExceptionWithContext> localExceptions = new ArrayList<>();
		boolean truncated = false;
		
		for (ExecutionVertex task : graph.getAllExecutionVertices()) {
			Throwable t = task.getFailureCause();
			if (t != null) {
				if (localExceptions.size() >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
					truncated = true;
					break;
				}

				InstanceConnectionInfo location = task.getCurrentAssignedResourceLocation();
				String locationString = location != null ?
						location.getFQDNHostname() + ':' + location.dataPort() :
						"(unassigned)";
				
				localExceptions.add(new ExceptionWithContext(t, task.getTaskName(), locationString));
			}
		}
		
		// if only one exception occurred in a task, and that is the root exception,
		// there is no need to display it twice
		if (localExceptions.size() == 1 && localExceptions.get(0).getException() == rootException) {
			localExceptions = null;
		}
		
		return JsonFactory.generateExceptionsJson(rootException, localExceptions, truncated);
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Class that encapsulated an exception, together with the name of the throwing task, and
	 * the instance on which the exception occurred.
	 */
	public static class ExceptionWithContext {
		
		private final Throwable exception;
		private final String taskName;
		private final String location;

		public ExceptionWithContext(Throwable exception, String taskName, String location) {
			this.exception = exception;
			this.taskName = taskName;
			this.location = location;
		}

		public Throwable getException() {
			return exception;
		}

		public String getTaskName() {
			return taskName;
		}

		public String getLocation() {
			return location;
		}
	}
}
