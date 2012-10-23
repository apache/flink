/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.checkpointing;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.util.StringUtils;

public final class CheckpointUtils {

	private static final Log LOG = LogFactory.getLog(CheckpointUtils.class);

	/**
	 * The prefix for the name of the file containing the checkpoint meta data.
	 */
	public static final String METADATA_PREFIX = "checkpoint";

	public static final String LOCAL_CHECKPOINT_PATH_KEY = "checkpoint.local.path";

	public static final String DISTRIBUTED_CHECKPOINT_PATH_KEY = "checkpoint.distributed.path";

	public static final String COMPLETED_CHECKPOINT_SUFFIX = "_final";

	private static Path LOCAL_CHECKPOINT_PATH = null;

	private static Path DISTRIBUTED_CHECKPOINT_PATH = null;

	private static CheckpointMode CHECKPOINT_MODE = null;

	private CheckpointUtils() {
	}

	public static Path getLocalCheckpointPath() {

		if (LOCAL_CHECKPOINT_PATH == null) {

			String localCheckpointPath = GlobalConfiguration.getString(LOCAL_CHECKPOINT_PATH_KEY, null);
			if (localCheckpointPath == null) {
				LOCAL_CHECKPOINT_PATH = new Path(new File(System.getProperty("java.io.tmpdir")).toURI());
			} else {
				LOCAL_CHECKPOINT_PATH = new Path(localCheckpointPath);
			}
		}

		return LOCAL_CHECKPOINT_PATH;
	}

	public static Path getDistributedCheckpointPath() {

		if (DISTRIBUTED_CHECKPOINT_PATH == null) {

			final String path = GlobalConfiguration.getString(DISTRIBUTED_CHECKPOINT_PATH_KEY, null);
			if (path == null) {
				return null;
			}

			DISTRIBUTED_CHECKPOINT_PATH = new Path(path);
		}

		return DISTRIBUTED_CHECKPOINT_PATH;
	}

	public static boolean hasCompleteCheckpointAvailable(final ExecutionVertexID vertexID) {

		return checkForCheckpoint(vertexID, COMPLETED_CHECKPOINT_SUFFIX);
	}

	public static boolean hasPartialCheckpointAvailable(final ExecutionVertexID vertexID) {

		if (checkForCheckpoint(vertexID, "_0")) {
			return true;
		}

		return checkForCheckpoint(vertexID, "_part");
	}

	public static boolean hasLocalCheckpointAvailable(final ExecutionVertexID vertexID) {

		try {
			Path local = new Path(getLocalCheckpointPath() + Path.SEPARATOR + METADATA_PREFIX + "_" + vertexID
				+ "_0");

			final FileSystem localFs = local.getFileSystem();

			if (localFs.exists(local)) {
				return true;
			}

			local = new Path(getLocalCheckpointPath() + Path.SEPARATOR + METADATA_PREFIX + "_" + vertexID
				+ "_part");

			return localFs.exists(local);

		} catch (IOException ioe) {
			LOG.warn(StringUtils.stringifyException(ioe));
		}

		return false;
	}

	private static boolean checkForCheckpoint(final ExecutionVertexID vertexID, final String suffix) {

		try {

			final Path local = new Path(getLocalCheckpointPath() + Path.SEPARATOR + METADATA_PREFIX + "_" + vertexID
				+ suffix);

			final FileSystem localFs = local.getFileSystem();

			if (localFs.exists(local)) {
				return true;
			}

			if (!allowDistributedCheckpoints()) {
				return false;
			}

			final Path distributedCheckpointPath = getDistributedCheckpointPath();
			if (distributedCheckpointPath == null) {
				return false;
			}

			final Path distributed = new Path(distributedCheckpointPath + Path.SEPARATOR + METADATA_PREFIX + "_"
				+ vertexID
				+ suffix);

			final FileSystem distFs = distributed.getFileSystem();

			return distFs.exists(distributed);

		} catch (IOException ioe) {
			LOG.warn(StringUtils.stringifyException(ioe));
		}

		return false;
	}

	/**
	 * Removes the checkpoint of the vertex with the given ID. All files contained in the checkpoint are deleted.
	 * 
	 * @param vertexID
	 *        the vertex whose checkpoint shall be removed
	 */
	public static void removeCheckpoint(final ExecutionVertexID vertexID) {

		final Path localChPath = getLocalCheckpointPath();

		try {
			if (!removeCheckpointMetaData(new Path(localChPath + Path.SEPARATOR + METADATA_PREFIX + "_" + vertexID))) {

				final Path distributedChPath = getDistributedCheckpointPath();
				if (distributedChPath != null && allowDistributedCheckpoints()) {
					removeCheckpointMetaData(new Path(distributedChPath + Path.SEPARATOR + METADATA_PREFIX + "_"
						+ vertexID));
				}
			}

			FileBufferManager.deleteFile(vertexID);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean removeCheckpointMetaData(final Path pathPrefix) throws IOException {

		boolean removed = false;

		Path p = pathPrefix.suffix("_part");
		FileSystem fs = p.getFileSystem();
		if (fs.exists(p)) {
			fs.delete(p, false);
			removed = true;
		}

		int suffix = 0;
		while (true) {
			p = pathPrefix.suffix("_" + suffix++);
			if (fs.exists(p)) {
				fs.delete(p, false);
				removed = true;
			} else {
				break;
			}
		}

		p = pathPrefix.suffix(COMPLETED_CHECKPOINT_SUFFIX);
		if (fs.exists(p)) {
			fs.delete(p, false);
			removed = true;
		}

		return removed;
	}

	public static CheckpointMode getCheckpointMode() {

		if (CHECKPOINT_MODE == null) {

			final String mode = GlobalConfiguration.getString("checkpoint.mode", "never").toLowerCase();
			if ("always".equals(mode)) {
				CHECKPOINT_MODE = CheckpointMode.ALWAYS;
			} else if ("network".equals(mode)) {
				CHECKPOINT_MODE = CheckpointMode.NETWORK;
			} else {
				CHECKPOINT_MODE = CheckpointMode.NEVER;
			}
		}

		return CHECKPOINT_MODE;
	}

	public static boolean allowDistributedCheckpoints() {

		return false;
	}
}
