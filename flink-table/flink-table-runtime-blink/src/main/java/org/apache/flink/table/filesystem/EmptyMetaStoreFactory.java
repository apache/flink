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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * Empty implementation {@link TableMetaStoreFactory}.
 */
@Internal
public class EmptyMetaStoreFactory implements TableMetaStoreFactory {

	private final Path path;

	public EmptyMetaStoreFactory(Path path) {
		this.path = path;
	}

	@Override
	public TableMetaStore createTableMetaStore() {
		return new TableMetaStore() {

			@Override
			public Path getLocationPath() {
				return path;
			}

			@Override
			public Optional<Path> getPartition(LinkedHashMap<String, String> partitionSpec) {
				return Optional.empty();
			}

			@Override
			public void createOrAlterPartition(
					LinkedHashMap<String, String> partitionSpec,
					Path partitionPath) {
			}

			@Override
			public void close() {

			}
		};
	}
}
