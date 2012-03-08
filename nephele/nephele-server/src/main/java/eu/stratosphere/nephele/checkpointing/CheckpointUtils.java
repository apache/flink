/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.util.StringUtils;

public final class CheckpointUtils {

	private static final Log LOG = LogFactory.getLog(CheckpointUtils.class);

	/**
	 * The prefix for the name of the file containing the checkpoint meta data.
	 */
	public static final String METADATA_PREFIX = "checkpoint";

	public static final String LOCAL_CHECKPOINT_PATH_KEY = "checkpoint.local.path";

	public static final String DISTRIBUTED_CHECKPOINT_PATH_KEY = "checkpoint.distributed.path";

	public static final String DEFAULT_LOCAL_CHECKPOINT_PATH = "file:///tmp";

	public static final String COMPLETED_CHECKPOINT_SUFFIX = "_final";

	private static Path LOCAL_CHECKPOINT_PATH = null;

	private static Path DISTRIBUTED_CHECKPOINT_PATH = null;

	private static double CP_UPPER = -1.0;

	private static double CP_LOWER = -1.0;

	private static CheckpointMode CHECKPOINT_MODE = null;

	private CheckpointUtils() {
	}

	public static Path getLocalCheckpointPath() {

		if (LOCAL_CHECKPOINT_PATH == null) {
			LOCAL_CHECKPOINT_PATH = new Path(GlobalConfiguration.getString(LOCAL_CHECKPOINT_PATH_KEY,
				DEFAULT_LOCAL_CHECKPOINT_PATH));
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
			removeCheckpoint(new Path(localChPath + Path.SEPARATOR + METADATA_PREFIX));

			final Path distributedChPath = getDistributedCheckpointPath();
			if (distributedChPath != null) {
				removeCheckpoint(new Path(distributedChPath + Path.SEPARATOR + METADATA_PREFIX));
			}
		} catch (IOException e) {
		}
	}

	private static void removeCheckpoint(final Path pathPrefix) throws IOException {

		Path p = pathPrefix.suffix(COMPLETED_CHECKPOINT_SUFFIX);
		FileSystem fs = p.getFileSystem();
		if (fs.exists(p)) {
			fs.delete(p, false);
			return;
		}

		p = pathPrefix.suffix("_0");
		if (fs.exists(p)) {
			fs.delete(p, false);
		}

		p = pathPrefix.suffix("_part");
		if (fs.exists(p)) {
			fs.delete(p, false);
		}
	}

	public static CheckpointMode getCheckpointMode() {

		if (CHECKPOINT_MODE == null) {

			final String mode = GlobalConfiguration.getString("checkpoint.mode", "never").toLowerCase();
			if ("always".equals(mode)) {
				CHECKPOINT_MODE = CheckpointMode.ALWAYS;
			} else if ("network".equals(mode)) {
				CHECKPOINT_MODE = CheckpointMode.NETWORK;
			} else if ("dynamic".equals(mode)) {
				CHECKPOINT_MODE = CheckpointMode.DYNAMIC;
			} else {
				CHECKPOINT_MODE = CheckpointMode.NEVER;
			}
		}

		return CHECKPOINT_MODE;
	}

	public static double getCPLower() {

		if (CP_LOWER < 0.0f) {
			CP_LOWER = Double.parseDouble(GlobalConfiguration.getString("checkpoint.lowerbound", "0.9"));
		}

		return CP_LOWER;
	}

	public static double getCPUpper() {

		if (CP_UPPER < 0.0f) {
			CP_UPPER = Double.parseDouble(GlobalConfiguration.getString("checkpoint.upperbound", "0.9"));
		}

		return CP_UPPER;
	}

	public static boolean usePACT() {

		return GlobalConfiguration.getBoolean("checkpoint.usepact", false);
	}

	public static boolean useAVG() {

		return GlobalConfiguration.getBoolean("checkpoint.useavg", false);
	}

	public static boolean createDistributedCheckpoint() {

		return GlobalConfiguration.getBoolean("checkpoint.distributed", false);
	}

	public static String getSummary() {

		return "Checkpointing Summary: UpperBound=" + getCPUpper() + " LowerBound=" + getCPLower()
			+ " ForcedValues: usePACT=" + usePACT() + " useAVG=" + useAVG()
			+ " mode=" + getCheckpointMode();
	}
}
