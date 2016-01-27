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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory for savepoint {@link StateStore} instances.
 */
public class SavepointStoreFactory {

	public static final String SAVEPOINT_BACKEND_KEY = "savepoints.state.backend";
	public static final String SAVEPOINT_DIRECTORY_KEY = "savepoints.state.backend.fs.dir";

	public static final Logger LOG = LoggerFactory.getLogger(SavepointStoreFactory.class);

	/**
	 * Creates a {@link SavepointStore} from the specified Configuration.
	 *
	 * <p>You can configure a savepoint-specific backend for the savepoints. If
	 * you don't configure anything, the regular checkpoint backend will be
	 * used.
	 *
	 * <p>The default and fallback backend is the job manager, which loses the
	 * savepoint after the job manager shuts down.
	 *
	 * @param config The configuration to parse the savepoint backend configuration.
	 * @return A savepoint store.
	 */
	public static SavepointStore createFromConfig(
			Configuration config) throws Exception {

		// Try a the savepoint-specific configuration first.
		String savepointBackend = config.getString(SAVEPOINT_BACKEND_KEY, null);

		if (savepointBackend == null) {
			LOG.info("No savepoint state backend configured. " +
					"Using job manager savepoint state backend.");
			return createJobManagerSavepointStore();
		}
		else if (savepointBackend.equals("jobmanager")) {
			LOG.info("Using job manager savepoint state backend.");
			return createJobManagerSavepointStore();
		}
		else if (savepointBackend.equals("filesystem")) {
			// Sanity check that the checkpoints are not stored on the job manager only
			String checkpointBackend = config.getString(
					ConfigConstants.STATE_BACKEND, "jobmanager");

			if (checkpointBackend.equals("jobmanager")) {
				LOG.warn("The combination of file system backend for savepoints and " +
						"jobmanager backend for checkpoints does not work. The savepoint " +
						"will *not* be recoverable after the job manager shuts down. " +
						"Falling back to job manager savepoint state backend.");

				return createJobManagerSavepointStore();
			}
			else {
				String rootPath = config.getString(SAVEPOINT_DIRECTORY_KEY, null);

				if (rootPath == null) {
					LOG.warn("Using filesystem as savepoint state backend, " +
							"but did not specify directory. Please set the " +
							"following configuration key: '" + SAVEPOINT_DIRECTORY_KEY +
							"' (e.g. " + SAVEPOINT_DIRECTORY_KEY + ": hdfs:///flink/savepoints/). " +
							"Falling back to job manager savepoint backend.");

					return createJobManagerSavepointStore();
				}
				else {
					LOG.info("Using filesystem savepoint backend (root path: {}).", rootPath);

					return createFileSystemSavepointStore(rootPath);
				}
			}
		}
		else {
			// Fallback
			LOG.warn("Unexpected savepoint backend configuration '{}'. " +
					"Falling back to job manager savepoint state backend.", savepointBackend);

			return createJobManagerSavepointStore();
		}
	}

	// ------------------------------------------------------------------------
	// Savepoint stores
	// ------------------------------------------------------------------------

	private static SavepointStore createJobManagerSavepointStore() {
		return new SavepointStore(new HeapStateStore<Savepoint>());
	}

	private static SavepointStore createFileSystemSavepointStore(String rootPath) throws IOException {
		return new SavepointStore(new FileSystemStateStore<Savepoint>(rootPath, "savepoint-"));
	}

}
