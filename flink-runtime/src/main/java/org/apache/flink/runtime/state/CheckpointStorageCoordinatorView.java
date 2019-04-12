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

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This interface creates a {@link CheckpointStorageLocation} to which
 * an individual checkpoint or savepoint is stored.
 *
 * <p>Methods of this interface act as an administration role in checkpoint coordinator.
 */
public interface CheckpointStorageCoordinatorView {

	/**
	 * Checks whether this backend supports highly available storage of data.
	 *
	 * <p>Some state backends may not support highly-available durable storage, with default settings,
	 * which makes them suitable for zero-config prototyping, but not for actual production setups.
	 */
	boolean supportsHighlyAvailableStorage();

	/**
	 * Checks whether the storage has a default savepoint location configured.
	 */
	boolean hasDefaultSavepointLocation();

	/**
	 * Resolves the given pointer to a checkpoint/savepoint into a checkpoint location. The location
	 * supports reading the checkpoint metadata, or disposing the checkpoint storage location.
	 *
	 * <p>If the state backend cannot understand the format of the pointer (for example because it
	 * was created by a different state backend) this method should throw an {@code IOException}.
	 *
	 * @param externalPointer The external checkpoint pointer to resolve.
	 * @return The checkpoint location handle.
	 *
	 * @throws IOException Thrown, if the state backend does not understand the pointer, or if
	 *                     the pointer could not be resolved due to an I/O error.
	 */
	CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;

	/**
	 * Initializes a storage location for new checkpoint with the given ID.
	 *
	 * <p>The returned storage location can be used to write the checkpoint data and metadata
	 * to and to obtain the pointers for the location(s) where the actual checkpoint data should be
	 * stored.
	 *
	 * @param checkpointId The ID (logical timestamp) of the checkpoint that should be persisted.
	 * @return A storage location for the data and metadata of the given checkpoint.
	 *
	 * @throws IOException Thrown if the storage location cannot be initialized due to an I/O exception.
	 */
	CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException;

	/**
	 * Initializes a storage location for new savepoint with the given ID.
	 *
	 * <p>If an external location pointer is passed, the savepoint storage location
	 * will be initialized at the location of that pointer. If the external location pointer is null,
	 * the default savepoint location will be used. If no default savepoint location is configured,
	 * this will throw an exception. Whether a default savepoint location is configured can be
	 * checked via {@link #hasDefaultSavepointLocation()}.
	 *
	 * @param checkpointId The ID (logical timestamp) of the savepoint's checkpoint.
	 * @param externalLocationPointer Optionally, a pointer to the location where the savepoint should
	 *                                be stored. May be null.
	 *
	 * @return A storage location for the data and metadata of the savepoint.
	 *
	 * @throws IOException Thrown if the storage location cannot be initialized due to an I/O exception.
	 */
	CheckpointStorageLocation initializeLocationForSavepoint(
		long checkpointId,
		@Nullable String externalLocationPointer) throws IOException;
}
