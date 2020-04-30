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

package org.apache.flink.fs.anotherdummy;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory of dummy FileSystem. See documentation of {@link AnotherDummyFSFileSystem}.
 */
public class AnotherDummyFSFactory implements FileSystemFactory {

	private final FileSystem fileSystem = new AnotherDummyFSFileSystem(getData());

	@Override
	public String getScheme() {
		return AnotherDummyFSFileSystem.FS_URI.getScheme();
	}

	@Override
	public FileSystem create(URI fsUri) {
		String dummyFileSystemClassName = "org.apache.flink.fs.dummy.DummyFSFileSystem";
		try {
			this.getClassLoader().loadClass(dummyFileSystemClassName);
			throw new RuntimeException(String.format("Class %s should not be visible for classloader of %s",
				dummyFileSystemClassName, this.getClass().getCanonicalName()));
		} catch (ClassNotFoundException e) {
			// Expected exception.
		}
		return fileSystem;
	}

	private static Map<String, String> getData() {
		Map<String, String> data = new HashMap<>();
		data.put("/words", "Hello World how are you, my dear dear world\n");
		return data;
	}
}
