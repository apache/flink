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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * A state handle that points to state in a file system, accessible as an input stream.
 */
public class FileStreamStateHandle extends AbstractFileStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = -6826990484549987311L;

	/**
	 * Creates a new FileStreamStateHandle pointing to state at the given file path.
	 * 
	 * @param filePath The path to the file containing the checkpointed state.
	 */
	public FileStreamStateHandle(Path filePath) {
		super(filePath);
	}

	@Override
	public InputStream getState(ClassLoader userCodeClassLoader) throws Exception {
		ensureNotClosed();

		InputStream inStream = getFileSystem().open(getFilePath());
		// make sure the state handle is cancelable
		registerCloseable(inStream);

		return inStream; 
	}

	/**
	 * Returns the file size in bytes.
	 *
	 * @return The file size in bytes.
	 * @throws IOException Thrown if the file system cannot be accessed.
	 */
	@Override
	public long getStateSize() throws IOException {
		return getFileSize();
	}

	@Override
	public <T extends Serializable> StateHandle<T> toSerializableHandle() {
		FileSerializableStateHandle<T> handle = new FileSerializableStateHandle<>(getFilePath());

		// forward closed status
		if (isClosed()) {
			try {
				handle.close();
			} catch (IOException e) {
				// should not happen on a fresh handle, but forward anyways
				throw new RuntimeException(e);
			}
		}

		return handle;
	}
}
