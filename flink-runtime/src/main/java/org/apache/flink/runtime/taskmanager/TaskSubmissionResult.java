/**
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


package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.executiongraph.ExecutionVertexID;

/**
 * A <code>TaskSubmissionResult</code> is used to report the results
 * of a task submission. It contains the ID of the submitted task, a return code and
 * a description. In case of a submission error the description includes an error message.
 * 
 */
public class TaskSubmissionResult extends AbstractTaskResult {

	/**
	 * Constructs a new task submission result.
	 * 
	 * @param vertexID
	 *        the task ID this result belongs to
	 * @param returnCode
	 *        the return code of the submission
	 */
	public TaskSubmissionResult(ExecutionVertexID vertexID, ReturnCode returnCode) {
		super(vertexID, returnCode);
	}

	/**
	 * Constructs an empty task submission result.
	 */
	public TaskSubmissionResult() {
		super();
	}
}
