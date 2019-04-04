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

package org.apache.flink.fs.azurefs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract factory for AzureFS. Subclasses override to specify
 * the correct scheme (wasb / wasbs).
 */
public abstract class AbstractAzureFSFactory implements FileSystemFactory {
	private static final Logger LOG = LoggerFactory.getLogger(AzureFSFactory.class);

	private Configuration flinkConfig;

	@Override
	public void configure(Configuration config) {
		flinkConfig = config;
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		checkNotNull(fsUri, "fsUri");
		LOG.info("Trying to load and instantiate Azure File System");
		return new AzureFileSystem(fsUri, flinkConfig);
	}
}
