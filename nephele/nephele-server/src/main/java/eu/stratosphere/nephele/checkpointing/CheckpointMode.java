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
 * This enumeration describes the different modes in which Nephele's checkpoints can be configured.
 * 
 * @author warneke
 */
public enum CheckpointMode {

	/**
	 * Never create any checkpoint.
	 */
	NEVER,

	/**
	 * Create a checkpoint whenever possible.
	 */
	ALWAYS,

	/**
	 * Create a checkpoint if at least one output of the task is transfered over the network.
	 */
	NETWORK,

	/**
	 * Creates checkpoints according to Nephele's internal strategies.
	 */
	DYNAMIC;
}
