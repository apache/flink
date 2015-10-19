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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.InstantiationUtil;

import java.io.ObjectInputStream;

/**
 * A state handle that points to state stored in a file via Java Serialization.
 * 
 * @param <T> The type of state pointed to by the state handle.
 */
public class FileSerializableStateHandle<T> extends AbstractFileState implements StateHandle<T> {

	private static final long serialVersionUID = -657631394290213622L;
	
	/**
	 * Creates a new FileSerializableStateHandle pointing to state at the given file path.
	 * 
	 * @param filePath The path to the file containing the checkpointed state.
	 */
	public FileSerializableStateHandle(Path filePath) {
		super(filePath);
	}

	@Override
	@SuppressWarnings("unchecked")
	public T getState(ClassLoader classLoader) throws Exception {
		FSDataInputStream inStream = getFileSystem().open(getFilePath());
		ObjectInputStream ois = new InstantiationUtil.ClassLoaderObjectInputStream(inStream, classLoader);
		return (T) ois.readObject();
	}
}
