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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests how DELETE requests behave.
 */
public class BlobServerDfsDeleteTest extends BlobServerDeleteTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Override
	protected Configuration getConfiguration() {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY,
			"dfs://" + temporaryFolder.getRoot().getPath());
		return config;
	}

	protected void cleanup(BlobServer server, BlobClient client, BlobCache cache) {
		cleanup(server, client);
		if (cache != null) {
			cache.shutdown();
		}
	}
}
