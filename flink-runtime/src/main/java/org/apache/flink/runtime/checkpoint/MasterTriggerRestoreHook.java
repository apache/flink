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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The interface for hooks that can be called by the checkpoint coordinator when triggering or
 * restoring a checkpoint. Such a hook is useful for example when preparing external systems for
 * taking or restoring checkpoints.
 *
 * <p>The {@link #triggerCheckpoint(long, long, Executor)} method (called when triggering a checkpoint)
 * can return a result (via a future) that will be stored as part of the checkpoint metadata.
 * When restoring a checkpoint, that stored result will be given to the {@link #restoreCheckpoint(long, Object)}
 * method. The hook's {@link #getIdentifier() identifier} is used to map data to hook in the presence
 * of multiple hooks, and when resuming a savepoint that was potentially created by a different job.
 * The identifier has a similar role as for example the operator UID in the streaming API.
 *
 * <p>It is possible that a job fails (and is subsequently restarted) before any checkpoints were successful.
 * In that situation, the checkpoint coordination calls {@link #reset()} to give the hook an
 * opportunity to, for example, reset an external system to initial conditions.
 *
 * <p>The MasterTriggerRestoreHook is defined when creating the streaming dataflow graph. It is attached
 * to the job graph, which gets sent to the cluster for execution. To avoid having to make the hook
 * itself serializable, these hooks are attached to the job graph via a {@link MasterTriggerRestoreHook.Factory}.
 *
 * @param <T> The type of the data produced by the hook and stored as part of the checkpoint metadata.
 *            If the hook never stores any data, this can be typed to {@code Void}.
 */
public interface MasterTriggerRestoreHook<T> {

	/**
	 * Gets the identifier of this hook. The identifier is used to identify a specific hook in the
	 * presence of multiple hooks and to give it the correct checkpointed data upon checkpoint restoration.
	 *
	 * <p>The identifier should be unique between different hooks of a job, but deterministic/constant
	 * so that upon resuming a savepoint, the hook will get the correct data.
	 * For example, if the hook calls into another storage system and persists namespace/schema specific
	 * information, then the name of the storage system, together with the namespace/schema name could
	 * be an appropriate identifier.
	 *
	 * <p>When multiple hooks of the same name are created and attached to a job graph, only the first
	 * one is actually used. This can be exploited to deduplicate hooks that would do the same thing.
	 *
	 * @return The identifier of the hook.
	 */
	String getIdentifier();

	/**
	 * This method is called by the checkpoint coordinator to reset the hook when
	 * execution is restarted in the absence of any checkpoint state.
	 *
	 * @throws Exception Exceptions encountered when calling the hook will cause execution to fail.
	 */
	default void reset() throws Exception {

	}

	/**
	 * Tear-down method for the hook.
	 *
	 * @throws Exception Exceptions encountered when calling close will be logged.
	 */
	default void close() throws Exception {

	}

	/**
	 * This method is called by the checkpoint coordinator prior when triggering a checkpoint, prior
	 * to sending the "trigger checkpoint" messages to the source tasks.
	 *
	 * <p>If the hook implementation wants to store data as part of the checkpoint, it may return
	 * that data via a future, otherwise it should return null. The data is stored as part of
	 * the checkpoint metadata under the hooks identifier (see {@link #getIdentifier()}).
	 *
	 * <p>If the action by this hook needs to be executed synchronously, then this method should
	 * directly execute the action synchronously. The returned future (if any) would typically be a
	 * completed future.
	 *
	 * <p>If the action should be executed asynchronously and only needs to complete before the
	 * checkpoint is considered completed, then the method may use the given executor to execute the
	 * actual action and would signal its completion by completing the future. For hooks that do not
	 * need to store data, the future would be completed with null.
	 *
	 * <p>Please note that this method should be non-blocking. Any heavy operation like IO operation
	 * should be executed asynchronously with given executor.
	 *
	 * @param checkpointId The ID (logical timestamp, monotonously increasing) of the checkpoint
	 * @param timestamp The wall clock timestamp when the checkpoint was triggered, for
	 *                  info/logging purposes.
	 * @param executor The executor for asynchronous actions
	 *
	 * @return Optionally, a future that signals when the hook has completed and that contains
	 *         data to be stored with the checkpoint.
	 *
	 * @throws Exception Exceptions encountered when calling the hook will cause the checkpoint to abort.
	 */
	@Nullable
	CompletableFuture<T> triggerCheckpoint(long checkpointId, long timestamp, Executor executor) throws Exception;

	/**
	 * This method is called by the checkpoint coordinator prior to restoring the state of a checkpoint.
	 * If the checkpoint did store data from this hook, that data will be passed to this method.
	 *
	 * @param checkpointId The ID (logical timestamp) of the restored checkpoint
	 * @param checkpointData The data originally stored in the checkpoint by this hook, possibly null.
	 *
	 * @throws Exception Exceptions thrown while restoring the checkpoint will cause the restore
	 *                   operation to fail and to possibly fall back to another checkpoint.
	 */
	void restoreCheckpoint(long checkpointId, @Nullable T checkpointData) throws Exception;

	/**
	 * Creates a the serializer to (de)serializes the data stored by this hook. The serializer
	 * serializes the result of the Future returned by the {@link #triggerCheckpoint(long, long, Executor)}
	 * method, and deserializes the data stored in the checkpoint into the object passed to the
	 * {@link #restoreCheckpoint(long, Object)} method.
	 *
	 * <p>If the hook never returns any data to be stored, then this method may return null as the
	 * serializer.
	 *
	 * @return The serializer to (de)serializes the data stored by this hook
	 */
	@Nullable
	SimpleVersionedSerializer<T> createCheckpointDataSerializer();

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * A factory to instantiate a {@code MasterTriggerRestoreHook}.
	 *
	 * <p>The hooks are defined when creating the streaming dataflow graph and are attached
	 * to the job graph, which gets sent to the cluster for execution. To avoid having to make
	 * the hook implementation serializable, a serializable hook factory is actually attached to the
	 * job graph instead of the hook implementation itself.
	 */
	interface Factory extends java.io.Serializable {

		/**
		 * Instantiates the {@code MasterTriggerRestoreHook}.
		 */
		<V> MasterTriggerRestoreHook<V> create();
	}
}
