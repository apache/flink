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

import org.apache.flink.util.FileUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This state handle represents a directory. This class is, for example, used to represent the directory of RocksDB's
 * native checkpoint directories for local recovery.
 */
public class DirectoryStateHandle implements StateObject {

	/** Serial version. */
	private static final long serialVersionUID = 1L;

	/** The path that describes the directory, as a string, to be serializable. */
	private final String directoryString;

	/** Transient path cache, to avoid re-parsing the string. */
	private transient Path directory;

	public DirectoryStateHandle(@Nonnull Path directory) {
		this.directory = directory;
		this.directoryString = directory.toString();
	}

	@Override
	public void discardState() throws IOException {
		ensurePath();
		FileUtils.deleteDirectory(directory.toFile());
	}

	@Override
	public long getStateSize() {
		// For now, we will not report any size, but in the future this could (if needed) return the total dir size.
		return 0L; // unknown
	}

	@Nonnull
	public Path getDirectory() {
		ensurePath();
		return directory;
	}

	private void ensurePath() {
		if (directory == null) {
			directory = Paths.get(directoryString);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DirectoryStateHandle that = (DirectoryStateHandle) o;

		return directoryString.equals(that.directoryString);
	}

	@Override
	public int hashCode() {
		return directoryString.hashCode();
	}

	@Override
	public String toString() {
		return "DirectoryStateHandle{" +
			"directory=" + directoryString +
			'}';
	}
}
