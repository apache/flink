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
import java.io.OutputStream;

public interface CheckpointStreamFactory {

	/**
	 * Creates an new {@link CheckpointStateOutputStream}. When the stream
	 * is closed, it returns a state handle that can retrieve the state back.
	 *
	 * @param checkpointID The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 *
	 * @return An output stream that writes state for the given checkpoint.
	 *
	 * @throws Exception Exceptions may occur while creating the stream and should be forwarded.
	 */
	CheckpointStateOutputStream createCheckpointStateOutputStream(
			long checkpointID,
			long timestamp) throws Exception;

	/**
	 * Closes the stream factory, releasing all internal resources, but does not delete any
	 * persistent checkpoint data.
	 *
	 * @throws Exception Exceptions can be forwarded and will be logged by the system
	 */
	void close() throws Exception;

	/**
	 * A dedicated output stream that produces a {@link StreamStateHandle} when closed.
	 *
	 * <p>Note: This is an abstract class and not an interface because {@link OutputStream}
	 * is an abstract class.
	 */
	abstract class CheckpointStateOutputStream extends FSDataOutputStream {

		/**
		 * Closes the stream and gets a state handle that can create an input stream
		 * producing the data written to this stream.
		 *
		 * @return A state handle that can create an input stream producing the data written to this stream.
		 * @throws IOException Thrown, if the stream cannot be closed.
		 */
		public abstract StreamStateHandle closeAndGetHandle() throws IOException;
	}
}
