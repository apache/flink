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

package org.apache.flink.runtime.state;

/**
 * A handle to those states that are referenced by different checkpoints.
 *
 * <p> Each shared state handle is identified by a unique key. Two shared states
 * are considered equal if their keys are identical.
 *
 * <p> All shared states are registered at the {@link SharedStateRegistry} once
 * they are received by the {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}
 * and will be unregistered when the checkpoints are discarded. A shared state
 * will be discarded once it is not referenced by any checkpoint. A shared state
 * should not be referenced any more if it has been discarded.
 */
public interface SharedStateHandle extends StateObject {

	/**
	 * Return the identifier of the shared state.
	 */
	String getRegistrationKey();
}
