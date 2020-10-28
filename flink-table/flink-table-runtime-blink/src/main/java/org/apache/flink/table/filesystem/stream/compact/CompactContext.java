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

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * Context for {@link CompactReader} and {@link CompactWriter}.
 */
public interface CompactContext {

	static CompactContext create(
			Configuration config,
			FileSystem fileSystem,
			String partition,
			Path path) {
		return new CompactContextImpl(config, fileSystem, partition, path);
	}

	Configuration getConfig();

	FileSystem getFileSystem();

	String getPartition();

	Path getPath();

	/**
	 * Implementation of {@link CompactContext}.
	 */
	class CompactContextImpl implements CompactContext {

		private final Configuration config;
		private final FileSystem fileSystem;
		private final String partition;
		private final Path path;

		private CompactContextImpl(
				Configuration config,
				FileSystem fileSystem,
				String partition,
				Path path) {
			this.config = config;
			this.fileSystem = fileSystem;
			this.partition = partition;
			this.path = path;
		}

		@Override
		public Configuration getConfig() {
			return config;
		}

		@Override
		public FileSystem getFileSystem() {
			return fileSystem;
		}

		@Override
		public String getPartition() {
			return partition;
		}

		@Override
		public Path getPath() {
			return path;
		}
	}
}
