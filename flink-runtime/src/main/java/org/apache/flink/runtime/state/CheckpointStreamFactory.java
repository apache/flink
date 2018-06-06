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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A factory for checkpoint output streams, which are used to persist data for checkpoints.
 *
 * <p>Stream factories can be created from the {@link CheckpointStorage} through
 * {@link CheckpointStorage#resolveCheckpointStorageLocation(long, CheckpointStorageLocationReference)}.
 */
public interface CheckpointStreamFactory {

	/**
	 * Creates an new {@link CheckpointStateOutputStream}. When the stream
	 * is closed, it returns a state handle that can retrieve the state back.
	 *
	 * @param scope The state's scope, whether it is exclusive or shared.
	 * @return An output stream that writes state for the given checkpoint.
	 *
	 * @throws IOException Exceptions may occur while creating the stream and should be forwarded.
	 */
	CheckpointStateOutputStream createCheckpointStateOutputStream(CheckpointedStateScope scope) throws IOException;

	/**
	 * A dedicated output stream that produces a {@link StreamStateHandle} when closed.
	 *
	 * <p><b>Important:</b> When closing this stream after the successful case, you must
	 * call {@link #closeAndGetHandle()} - only that method will actually retain the resource
	 * written to. The method has the semantics of "close on success".
	 * The {@link #close()} method is supposed to remove the target resource if
	 * called before {@link #closeAndGetHandle()}, hence having the semantics of
	 * "close on failure". That way, simple try-with-resources statements automatically
	 * clean up unsuccessful partial state resources in case the writing does not complete.
	 *
	 * <p>Note: This is an abstract class and not an interface because {@link OutputStream}
	 * is an abstract class.
	 */
	abstract class CheckpointStateOutputStream extends FSDataOutputStream {

		/**
		 * Closes the stream and gets a state handle that can create an input stream
		 * producing the data written to this stream.
		 *
		 * <p>This closing must be called (also when the caller is not interested in the handle)
		 * to successfully close the stream and retain the produced resource. In contrast,
		 * the {@link #close()} method removes the target resource when called.
		 *
		 * @return A state handle that can create an input stream producing the data written to this stream.
		 * @throws IOException Thrown, if the stream cannot be closed.
		 */
		@Nullable
		public abstract StreamStateHandle closeAndGetHandle() throws IOException;

		/**
		 * This method should close the stream, if has not been closed before.
		 * If this method actually closes the stream, it should delete/release the
		 * resource behind the stream, such as the file that the stream writes to.
		 *
		 * <p>The above implies that this method is intended to be the "unsuccessful close",
		 * such as when cancelling the stream writing, or when an exception occurs.
		 * Closing the stream for the successful case must go through {@link #closeAndGetHandle()}.
		 *
		 * @throws IOException Thrown, if the stream cannot be closed.
		 */
		@Override
		public abstract void close() throws IOException;
	}
}
