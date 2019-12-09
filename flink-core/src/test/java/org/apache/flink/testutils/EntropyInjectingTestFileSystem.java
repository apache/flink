/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.testutils;

import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.net.URI;

/**
 * A test file system that implements {@link EntropyInjectingFileSystem}.
 */
public class EntropyInjectingTestFileSystem extends LocalFileSystem implements EntropyInjectingFileSystem {

	public static final String ENTROPY_INJECTION_KEY = "_entropy_";

	public static final String ENTROPY = "_resolved_";

	@Override
	public String getEntropyInjectionKey() {
		return ENTROPY_INJECTION_KEY;
	}

	@Override
	public String generateEntropy() {
		return ENTROPY;
	}

	public static class EntropyInjectingTestFileSystemFactory implements FileSystemFactory {

		@Override
		public String getScheme() {
			return "test-entropy";
		}

		@Override
		public FileSystem create(final URI fsUri) {
			return new EntropyInjectingTestFileSystem();
		}
	}
}
