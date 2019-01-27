/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The utils of external block shuffle.
 */
public class ExternalBlockShuffleUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockShuffleUtils.class);

	private static final String CONFIG_FILE = "config";

	private static final String DATA_FILE = "data";

	private static final String SPILL_FILE = "spill";

	private static final String INDEX_FILE = "index";

	private static final String MERGE_FILE = "merge";

	/** FINISHED_FILE is used to identify whether a result partition is finished and ready to be fetched. */
	private static final String FINISHED_FILE = "finished";

	/** The prefix of result partition's relative directory path. */
	private static final String PARTITION_DIR_PREFIX = "partition_";

	/** The splitter to separate producerId and partitionId in constructing partition director. */
	private static final String SPLITTER = "_";

	/** Generate result partition's directory based on the given root directory. */
	public static String generatePartitionRootPath(String prefix, String producerId, String partitionId) {
		return prefix + "/" + PARTITION_DIR_PREFIX + producerId + SPLITTER + partitionId + "/";
	}

	/** Convert relative partition directory to ResultPartitionID according to generatePartitionRootPath() method. */
	public static ResultPartitionID convertRelativeDirToResultPartitionID(String relativeDir) {
		if (!relativeDir.startsWith(PARTITION_DIR_PREFIX)) {
			return null;
		}
		String[] segments = relativeDir.substring(PARTITION_DIR_PREFIX.length()).split(SPLITTER);
		if (segments == null || segments.length != 2) {
			return null;
		}
		try {
			return new ResultPartitionID(
				new IntermediateResultPartitionID(DatatypeConverter.parseHexBinary(segments[1])),
				new ExecutionAttemptID(DatatypeConverter.parseHexBinary(segments[0])));
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Generate data file path.
	 *
	 * @param fileIndex
	 * 		In SINGLE_SUBPARTITION_FILE, fileIndex is subpartition index;
	 * 		In MULTI_SUBPARTITION_FILE, fileIndex is the spill index;
	 * @return Data file path.
	 */
	public static String generateDataPath(String partitionPrefix, int fileIndex) {
		return partitionPrefix + fileIndex + "." + DATA_FILE;
	}

	public static String generateSpillPath(String partitionPrefix, int fileIndex) {
		return partitionPrefix + fileIndex + "." + SPILL_FILE;
	}

	public static String generateMergePath(String partitionPrefix, int fileIndex) {
		return partitionPrefix + fileIndex + "." + MERGE_FILE;
	}

	/**
	 * Generate index file path, notice that only MULTI_SUBPARTITION_FILE mode need index files.
	 *
	 * @param partitionPrefix Root directory for this result partition.
	 * @param fileIndex FileIndex is the spill index.
	 * @return Index file path.
	 */
	public static String generateIndexPath(String partitionPrefix, int fileIndex) {
		return partitionPrefix + fileIndex + "." + INDEX_FILE;
	}

	public static String generateFinishedPath(String partitionPrefix) {
		return partitionPrefix + FINISHED_FILE;
	}

	public static String generateConfigPath(String partitionPrefix) {
		return partitionPrefix + CONFIG_FILE;
	}

	public static int hashPartitionToDisk(String producerId, String partitionId) {
		int hashCode = (producerId + partitionId).hashCode();
		hashCode = hashCode < 0 ? (-hashCode) : hashCode;
		return hashCode;
	}

	public static void serializeIndices(List<PartitionIndex> indices, DataOutputView out) throws IOException {
		checkNotNull(indices);
		out.writeInt(indices.size());
		for (PartitionIndex index : indices) {
			out.writeInt(index.getPartition());
			out.writeLong(index.getStartOffset());
			out.writeLong(index.getLength());
		}
	}

	public static PartitionIndex[] deserializeIndices(DataInputView in, int subpartitionNum) throws IOException {
		int size = in.readInt();
		if (subpartitionNum <= 0) {
			subpartitionNum = size;
		} else {
			assert subpartitionNum >= size;
		}
		PartitionIndex[] indices = new PartitionIndex[subpartitionNum];
		for (int i = 0; i < size; i++) {
			int partitionId = in.readInt();
			//ensure get the PartitionIndex by the array index
			PartitionIndex index = new PartitionIndex(
				partitionId, in.readLong(), in.readLong());
			indices[partitionId] = index;
		}
		return indices;
	}
}
