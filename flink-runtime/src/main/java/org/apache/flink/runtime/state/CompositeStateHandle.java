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
 * Base of all snapshots that are taken by {@link StateBackend}s and some other
 * components in tasks.
 *
 * <p>Each snapshot is composed of a collection of {@link StateObject}s. The
 * {@link StateObject}s in a completed checkpoint may be referenced by other
 * completed checkpoints. To avoid the deletion of those objects still in use,
 * the handle should register all its objects when the checkpoint completes and
 * unregister its objects when the checkpoint is discarded.
 */
public interface CompositeStateHandle extends StateObject {

	/**
	 * This method is called when the checkpoint is added into
	 * {@link org.apache.flink.runtime.checkpoint.CompletedCheckpointStore}.
	 * That happens when the pending checkpoint succeeds to complete or the
	 * completed checkpoint is reloaded in the recovery. In both cases, the
	 * snapshot handle should register all its objects in the given
	 * {@link StateRegistry}.
	 */
	void register(StateRegistry stateRegistry);

	/**
	 * This method is called when the completed checkpoint is discarded. In such
	 * cases, the snapshot handle should unregister all its objects. An object
	 * will be deleted if it is not referenced by any checkpoint.
	 */
	void unregister(StateRegistry stateRegistry);
}
