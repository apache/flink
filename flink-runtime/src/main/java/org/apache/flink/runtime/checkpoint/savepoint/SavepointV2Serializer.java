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

package org.apache.flink.runtime.checkpoint.savepoint;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

/**
 * A savepoint serializer that does not store absolute URIs for {@link FileStateHandle}
 * instances, allowing users to relocate savepoints as long as the file structure
 * within the savepoint directory stays the same.
 */
class SavepointV2Serializer extends AbstractSavepointSerializer<SavepointV2> {

	public static final SavepointV2Serializer INSTANCE = new SavepointV2Serializer();

	private SavepointV2Serializer() {
	}

	@Override
	SavepointV2 createSavepoint(long checkpointId, Collection<TaskState> taskStates) {
		return new SavepointV2(checkpointId, taskStates);
	}

	@Override
	void serializeFileStreamStateHandle(FileStateHandle fileStateHandle, Path basePath, DataOutputStream dos) throws IOException {
		dos.writeLong(fileStateHandle.getStateSize());

		Path child = fileStateHandle.getFilePath();
		Path relative = getRelativePath(basePath, child);

		if (relative != null) {
			// This boolean is new in this version of the serializer
			dos.writeBoolean(true);
			dos.writeUTF(relative.toString());
		} else {
			dos.writeBoolean(false);
			dos.writeUTF(fileStateHandle.getFilePath().toString());
		}
	}

	@Override
	FileStateHandle deserializeFileStreamStateHandle(Path basePath, DataInputStream dis) throws IOException {
		long size = dis.readLong();
		boolean isRelative = dis.readBoolean();
		String pathString = dis.readUTF();

		Path path = isRelative ? new Path(basePath, pathString) : new Path(pathString);
		return new FileStateHandle(path, size);
	}

	/**
	 * Returns the childPath relative to the basePath.
	 *
	 * <p>If the child path is not a child of the base path, <code>null</code>
	 * is returned.
	 *
	 * <pre>
	 * getRelativePath("/base", "/base/child") -> "child"
	 * getRelativePath("/base", "/base/parent/child") -> "parent/child"
	 * getRelativePath("/base", "/child-of-root") -> null
	 * getRelativePath("/base", "/other-base/child") -> null
	 * </pre>
	 *
	 * @return The relative child path against base or <code>null</code> if not an actual child of
	 * base.
	 * @throws NullPointerException If arguments are <code>null</code>
	 */
	@Nullable
	static Path getRelativePath(Path base, Path child) {
		URI baseUri = checkNotNull(base, "base").toUri();
		URI childUri = checkNotNull(child, "child").toUri();

		// Relativize against the base path
		URI relativeUri = baseUri.relativize(childUri);

		if (!relativeUri.equals(childUri)) {
			return new Path(relativeUri);
		} else {
			// If childUri is returned, childPath was not a child path of base
			return null;
		}
	}
}
