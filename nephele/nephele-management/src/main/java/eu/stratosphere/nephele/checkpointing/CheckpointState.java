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

package eu.stratosphere.nephele.checkpointing;

/**
 * This enumeration defines the possibles states a checkpoint of a task can be in.
 * 
 * @author warneke
 */
public enum CheckpointState {

	/**
	 * The vertex has no checkpoint and cannot be recovered.
	 */
	NONE,

	/**
	 * The checkpoint is currently being written. The checkpoint can be used for recovery as long as the task which
	 * initiated it keeps adding data to the checkpoint.
	 */
	PARTIAL,

	/**
	 * The checkpoint is complete and can be used for recovery no matter if the task that has previously created the
	 * checkpoint is still running.
	 */
	COMPLETE;
}
