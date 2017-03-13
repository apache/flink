/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

/**
 * Base for those state handles that are shared among different checkpoints.
 * 
 * Each shared state handle is identified by an unique key. It will be
 * registered at the {@link SharedStateRegistry} once it is received by the 
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}. Each 
 * registered state handle is unregistered when the checkpoint to which the 
 * state handle belongs is discarded.
 */
public interface SharedStateHandle extends StateObject {

	/**
	 * Returns the unique identifier of the state handle
	 * 
	 * @return the unique identifier of the state handle
	 */
	String getKey();
}
