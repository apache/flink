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


package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.JobID;

/**
 * This interface must be implemented in order to receive a notification whenever the job has entered a new execution
 * stage.
 * 
 */
public interface ExecutionStageListener {

	/**
	 * This method is called to indicate that the job with the given {@link JobID} has finished its previous
	 * {@link ExecutionStage} and has entered the next {@link ExecutionStage}. Note that a notification is not sent when
	 * the job has entered its initial execution stage.
	 * 
	 * @param jobID
	 *        the ID of the job the notification belongs to
	 * @param executionStage
	 *        the next execution stage that has just been entered
	 */
	void nextExecutionStageEntered(JobID jobID, ExecutionStage executionStage);

}
