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

import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;

import java.io.IOException;

/**
 * A storage location for one particular checkpoint. This location is typically
 * created and initialized via {@link CheckpointStorage#initializeLocationForCheckpoint(long)} or
 * {@link CheckpointStorage#initializeLocationForSavepoint(long, String)}.
 */
public interface CheckpointStorageLocation {

	/**
	 * Creates the output stream to persist the checkpoint metadata to.
	 *
	 * @return The output stream to persist the checkpoint metadata to.
	 * @throws IOException Thrown, if the stream cannot be opened due to an I/O error.
	 */
	CheckpointStateOutputStream createMetadataOutputStream() throws IOException;

	/**
	 * Finalizes the checkpoint, marking the location as a finished checkpoint.
	 * This method returns the external checkpoint pointer that can be used to resolve
	 * the checkpoint upon recovery.
	 *
	 * @return The external pointer to the checkpoint at this location.
	 * @throws IOException Thrown, if finalizing / marking as finished fails due to an I/O error.
	 */
	String markCheckpointAsFinished() throws IOException;

	/**
	 * Disposes the checkpoint location in case the checkpoint has failed.
	 */
	void disposeOnFailure() throws IOException;

	/**
	 * Gets the location encoded as a string pointer.
	 *
	 * <p>This pointer is used to send the target storage location via checkpoint RPC messages
	 * and checkpoint barriers, in a format avoiding backend-specific classes.
	 *
	 * <p>That string encodes the location typically in a backend-specific way.
	 * For example, file-based backends can encode paths here.
	 */
	String getLocationAsPointer();
}
