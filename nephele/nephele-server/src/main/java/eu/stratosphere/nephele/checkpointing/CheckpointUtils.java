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

import java.io.File;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;

public final class CheckpointUtils {

	/**
	 * The prefix for the name of the file containing the checkpoint meta data.
	 */
	public static final String METADATA_PREFIX = "checkpoint";

	public static final String CHECKPOINT_DIRECTORY_KEY = "channel.checkpoint.directory";

	public static final String DEFAULT_CHECKPOINT_DIRECTORY = "/tmp";

	private static String CHECKPOINT_DIRECTORY = null;

	private static double CP_UPPER = -1.0;

	private static double CP_LOWER = -1.0;

	private static CheckpointMode CHECKPOINT_MODE = null;

	private CheckpointUtils() {
	}

	static String getCheckpointDirectory() {

		if (CHECKPOINT_DIRECTORY == null) {
			CHECKPOINT_DIRECTORY = GlobalConfiguration
				.getString(CHECKPOINT_DIRECTORY_KEY, DEFAULT_CHECKPOINT_DIRECTORY);
		}

		return CHECKPOINT_DIRECTORY;
	}

	public static boolean hasCompleteCheckpointAvailable(final ExecutionVertexID vertexID) {

		final File file = new File(getCheckpointDirectory() + File.separator + METADATA_PREFIX + "_" + vertexID
			+ "_final");
		if (file.exists()) {
			return true;
		}

		return false;
	}

	public static boolean hasPartialCheckpointAvailable(final ExecutionVertexID vertexID) {

		File file = new File(getCheckpointDirectory() + File.separator + METADATA_PREFIX + "_" + vertexID + "_0");
		if (file.exists()) {
			return true;
		}

		file = new File(getCheckpointDirectory() + File.separator + METADATA_PREFIX + "_" + vertexID + "_part");
		if (file.exists()) {
			return true;
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

		final String checkpointDirectory = getCheckpointDirectory();

		File file = new File(checkpointDirectory + File.separator + METADATA_PREFIX + "_" + vertexID
				+ "_final");
		if (file.exists()) {
			file.delete();
			return;
		}
		file = new File(checkpointDirectory + File.separator + METADATA_PREFIX + "_" + vertexID + "_0");
		if (file.exists()) {
			file.delete();
		}

		file = new File(checkpointDirectory + File.separator + METADATA_PREFIX + "_" + vertexID + "_part");
		if (file.exists()) {
			file.delete();
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

		return GlobalConfiguration.getBoolean("checkpoint.distributed", true);
	}

	public static String getSummary() {

		return "Checkpointing Summary: UpperBound=" + getCPUpper() + " LowerBound=" + getCPLower()
			+ " ForcedValues: usePACT=" + usePACT() + " useAVG=" + useAVG()
			+ " mode=" + getCheckpointMode();
	}
}
