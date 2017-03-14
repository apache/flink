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

import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link CheckpointMetadataStreamFactory} creates streams to write checkpoint <i>metadata</i>,
 * similar as the {@link CheckpointStreamFactory} creates streams to write checkpoint <i>data</i>.
 */
public interface CheckpointMetadataStreamFactory {

	/**
	 * Creates a stream to write the metadata for the particular checkpoint that this factory
	 * belongs to.
	 * 
	 * @return The metadata output stream for this factory's checkpoint.
	 * @throws IOException Thrown, if the stream could not be opened.
	 */
	CheckpointMetadataOutputStream createCheckpointStateOutputStream() throws IOException;

	/**
	 * Gets the location (as a string pointer) where the metadata factory 
	 * The interpretation of the pointer is up to the implementation of the state backend.
	 * 
	 * <p>In case of high-availability setups, the target location pointer is stored
	 * in the "ground-truth" store for checkpoint recovery.
	 * 
	 * @return The checkpoint location pointer.
	 */
	String getTargetLocation();

	// ------------------------------------------------------------------------

	/**
	 * A dedicated output stream for persisting checkpoint metadata. Upon completion, this returns
	 * the state handle to the persisted metadata, plus the external pointer that can be used
	 * to restore from that checkpoint.
	 */
	abstract class CheckpointMetadataOutputStream extends FSDataOutputStream {

		/**
		 * Closes the metadata stream and gets the external pointer to the checkpoint and a
		 * handle that can create an input stream producing the data written to this stream.
		 *
		 * @return The pointer and state handle with access to the written metadata.
		 * @throws IOException Thrown, if the stream cannot be closed.
		 */
		public abstract StreamHandleAndPointer closeAndGetPointerHandle() throws IOException;
	}

	// ------------------------------------------------------------------------

	/**
	 * A combination of a {@code StreamStateHandle} and an external pointer (in the form of a String).
	 */
	final class StreamHandleAndPointer {

		private final StreamStateHandle stateHandle;

		private final String pointer;

		public StreamHandleAndPointer(StreamStateHandle stateHandle, String pointer) {
			this.stateHandle = checkNotNull(stateHandle);
			this.pointer = checkNotNull(pointer);
		}

		public StreamStateHandle stateHandle() {
			return stateHandle;
		}

		public String pointer() {
			return pointer;
		}

		@Override
		public String toString() {
			return pointer + " -> " + stateHandle;
		}
	}
}
