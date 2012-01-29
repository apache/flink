/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public interface Task {

	/**
	 * Returns the ID of the job this task belongs to.
	 * 
	 * @return the ID of the job this task belongs to
	 */
	JobID getJobID();

	/**
	 * Returns the ID of this task.
	 * 
	 * @return the ID of this task
	 */
	ExecutionVertexID getVertexID();

	/**
	 * Returns the environment associated with this task.
	 * 
	 * @return the environment associated with this task
	 */
	Environment getEnvironment();
}
