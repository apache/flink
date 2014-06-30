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

package org.apache.flink.runtime.jobgraph;

/**
 * Possible states of a job once it has been accepted by the job manager.
 */
public enum JobStatus {

	/** Job is newly created, no task has started to run. */
	CREATED,

	/** Some tasks are scheduled or running, some may be pending, some may be finished. */
	RUNNING,

	/** The job has failed to to non-recoverable task failure */
	FAILED,

	/** Job is being cancelled */
	CANCELLING,
	
	/** Job has been cancelled */
	CANCELED,

	/** All of the job's tasks have successfully finished. */
	FINISHED
};
