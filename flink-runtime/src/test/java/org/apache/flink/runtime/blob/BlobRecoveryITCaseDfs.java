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

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertNull;

public class BlobRecoveryITCaseDfs extends BlobRecoveryITCaseHA {

	@Override
	protected Configuration getConfiguration() {
		Configuration config = new Configuration();

		// since this directory will be deleted, create an individual one below
		// temporaryFolder:
		config.setString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY,
			"dfs://" + temporaryFolder.getRoot().getPath() + "/default");
		return config;
	}

	/**
	 * Verify everything is clean below <tt>recoveryDir/default</tt>.
	 *
	 * @param config
	 */
	@Override
	protected void verifyClean(Configuration config) {
		File haBlobStoreDir = new File(temporaryFolder.getRoot(), "default");
		File[] recoveryFiles = haBlobStoreDir.listFiles();
		assertNull("Unclean state backend: " + Arrays.toString(recoveryFiles), recoveryFiles);
	}
}
