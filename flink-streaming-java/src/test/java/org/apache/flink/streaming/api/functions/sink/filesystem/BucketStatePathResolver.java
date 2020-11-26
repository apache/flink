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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import java.nio.file.Path;

/**
 * Utilities to resolve the directory structure for the bucket state
 * generated for migration test.
 */
public class BucketStatePathResolver {
	private final java.nio.file.Path basePath;
	private final int version;

	public BucketStatePathResolver(Path basePath, int version) {
		this.basePath = basePath;
		this.version = version;
	}

	public java.nio.file.Path getSnapshotPath(String scenarioName) {
		java.nio.file.Path basePath = getResourcePath(scenarioName);
		return basePath.resolve("snapshot");
	}

	public java.nio.file.Path getOutputPath(String scenarioName) {
		java.nio.file.Path basePath = getResourcePath(scenarioName);
		return basePath.resolve("bucket");
	}

	public java.nio.file.Path getResourcePath(String scenarioName) {
		return basePath.resolve(scenarioName + "-v" + version);
	}
}
