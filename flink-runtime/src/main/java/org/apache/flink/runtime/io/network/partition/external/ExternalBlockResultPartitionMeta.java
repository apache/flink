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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

class ExternalBlockResultPartitionMeta {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockResultPartitionMeta.class);

	/** Supported version of result partition file in case of incompatible partition file. */
	public static final int SUPPORTED_PROTOCOL_VERSION = 1;

	private final ResultPartitionID resultPartitionID;

	/** File system of data and index file for this partition */
	private final FileSystem fileSystem;

	/** Make sure result partition meta will be initialized only once. */
	@GuardedBy("this")
	private volatile boolean hasInitialized = false;

	private PersistentFileType externalFileType = PersistentFileType.UNDEFINED;

	/**
	 * Whole partition indices, use an array of which each entry contain a list
	 * holding PartitionIndices of the corresponding subpartition. Furthermore
	 * we can just set null to make those unused PartitionIndices GC when memory gets tight.
	 */
	private List<ExternalSubpartitionMeta>[] subpartitionMetas;

	private final LocalResultPartitionResolver.ResultPartitionFileInfo fileInfo;

	/**
	 * Spill count actually shows the count of logical partitioned files.
	 * We can treat a logical partitioned file as a logical file that contains
	 * data from each subpartition. Each subpartition in a logical file should
	 * be continuously stored.
	 *
	 * <p>In SINGLE_SUBPARTITION_FILE there will be only one logical partitioned file.
	 *  So its spill count is one.
	 *
	 * <p>In MULTI_SUBPARTITION_FILE, each spill phase will generate a logical
	 *  partitioned file and may be multiple spill files left for shuffle service
	 *  to do the final concatenation for each subpartition.
	 *
	 * <p>In COMPACT_MULTI_SUBPARTITION_FILE, each spill phase will also generate
	 *  a logical partitioned file but will append to the file as same as the
	 *  previous spill phase. Shuffle service also need to do the concatenation
	 *  of subpartition's data just like in MULTI_SUBPARTITION_FILE mode.
	 */
	private int spillCount = 1;

	private int subpartitionNum = 0;

	/** How many subpartitions left unconsumed */
	private final AtomicInteger unconsumedSubpartitionCount = new AtomicInteger(Integer.MAX_VALUE);

	/** How many subpartition views alive */
	private final AtomicInteger refCount = new AtomicInteger(0);

	private final AtomicReference<Long> lastActiveTimeInMs = new AtomicReference<>(-1L);

	ExternalBlockResultPartitionMeta(
		ResultPartitionID resultPartitionID,
		FileSystem fileSystem,
		LocalResultPartitionResolver.ResultPartitionFileInfo fileInfo) {

		this.resultPartitionID = resultPartitionID;
		this.fileSystem = fileSystem;
		this.fileInfo = fileInfo;
	}

	boolean hasInitialized() {
		return hasInitialized;
	}

	synchronized void initialize() throws IOException {
		if (hasInitialized) {
			return;
		}

		// Then read finish file to get external file type
		initializeByFinishFile();

		// Initialize partition indices
		initializeByIndexFile();

		// Set reference count according to subpartition number
		unconsumedSubpartitionCount.set(subpartitionNum);
		lastActiveTimeInMs.set(System.currentTimeMillis());

		// As this method should read from storage, make sure it to be done once at most
		hasInitialized = true;
	}

	public String getRootDir() {
		return fileInfo.getRootDir();
	}

	public String getResultPartitionDir() {
		return fileInfo.getPartitionDir();
	}

	public synchronized List<ExternalSubpartitionMeta> getSubpartitionMeta(int subpartitionIndex) {
		checkState(hasInitialized, "The meta info has not been initialized.");
		checkArgument(subpartitionIndex >= 0 && subpartitionIndex < subpartitionNum, "Invalid subpartition index.");

		return subpartitionMetas[subpartitionIndex];
	}

	PersistentFileType getExternalBlockFileType() {
		if (hasInitialized()) {
			return externalFileType;
		} else {
			throw new RuntimeException("This method should be called after initialize()");
		}
	}

	long getConsumedPartitionTTL() {
		return fileInfo.getConsumedPartitionTTL();
	}

	long getPartialConsumedPartitionTTL() {
		return fileInfo.getPartialConsumedPartitionTTL();
	}

	void notifySubpartitionStartConsuming(int subpartitionIndex) {
		// Increase reference count
		lastActiveTimeInMs.set(System.currentTimeMillis());
		refCount.addAndGet(1);
	}

	/**
	 * Notify one subpartition finishes consuming this result partition.
	 * @param subpartitionIndex The index of the consumed subpartition.
	 */
	void notifySubpartitionConsumed(int subpartitionIndex) {
		// TODO: Maybe we need a bitmap to judge whether all the subpartitions have been consumed
		// since one subpartition may trigger multiple decrements due to downstream retries.
		// As a result, its reference will reach zero even if some subpartitions are unconsumed.
		// UnconsumedSubpartitionCount can be negative due to the same reason.
		// Currently we rely on consumedPartitionTTL to try to avoid such bad cases.
		long currTime = System.currentTimeMillis();
		if (unconsumedSubpartitionCount.decrementAndGet() < 1) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Partition {} 's reference count turn to zero at {}", resultPartitionID, currTime);
			}
		}

		// Decrease reference count.
		lastActiveTimeInMs.set(currTime);
		refCount.decrementAndGet();
	}

	int getReferenceCount() {
		return refCount.get();
	}

	int getUnconsumedSubpartitionCount() {
		// unconsumedSubpartitionCount can become a negative value due to reduce task rerun
		int count = unconsumedSubpartitionCount.get();
		return count >= 0 ? count : 0;
	}

	long getLastActiveTimeInMs() {
		return lastActiveTimeInMs.get();
	}

	@VisibleForTesting
	ResultPartitionID getResultPartitionID() {
		return resultPartitionID;
	}

	// -------------------------------- Internal Utilities ------------------------------------

	private void initializeByFinishFile() throws IOException {
		String finishedPathStr = ExternalBlockShuffleUtils.generateFinishedPath(fileInfo.getPartitionDir());
		Path finishFilePath = new Path(finishedPathStr);

		if (!fileSystem.exists(finishFilePath)) {
			throw new IOException("Finish file doesn't exist, file path: " + finishFilePath.getPath());
		}
		FSDataInputStream finishIn = null;
		try {
			finishIn = fileSystem.open(finishFilePath);
			if (null == finishIn) {
				throw new IOException("Cannot open finish file, file path: " + finishedPathStr);
			}
			DataInputView finishView = new DataInputViewStreamWrapper(finishIn);

			/**
			 *  Content of finish file:
			 *
			 *  |<- protocol version (4 bytes, an integer)  ->|
			 *  |<- length of "Type" field (4 bytes, an integer)  ->|
			 *  |<- variable-length based on previous integer (the value of "Type") ->|
			 *  |<- spill count (4 bytes, an integer) ->|
			 *  |<- subpartition number (4 bytes, an integer, since Version_02) ->|
			 */

			// Read protocol version
			int version = finishView.readInt();
			if (version > SUPPORTED_PROTOCOL_VERSION) {
				throw new RuntimeException("Unsupported External Data version: "
					+ version + ", supported version: " + SUPPORTED_PROTOCOL_VERSION);
			}

			// Read "Type" field
			int typeLength = finishView.readInt();
			byte[] typeContent = new byte[typeLength];
			finishView.read(typeContent);
			externalFileType = PersistentFileType.valueOf(new String(typeContent));

			// Read spill count.
			spillCount = finishView.readInt();
			if (externalFileType == PersistentFileType.HASH_PARTITION_FILE) {
				assert spillCount == 1;
			} else {
				// In MULTI_SUBPARTITION_FILE mode, spill count can be zero if there is
				// no records in this result partition.
				assert spillCount >= 0;
			}

			subpartitionNum = finishView.readInt();
			assert subpartitionNum > 0;
		} finally {
			// Close file handler
			if (null != finishIn) {
				finishIn.close();
			}
		}
	}

	private void initializeByIndexFile() throws IOException {
		PartitionIndex[][] partitionIndices = new PartitionIndex[spillCount][];
		Path[] dataFiles = new Path[spillCount];

		// load PartitionIndices from index files
		Path indexFilePath = null;
		FSDataInputStream indexIn = null;
		DataInputView indexView = null;
		try {
			for (int idx = 0; idx < spillCount; idx++) {
				if (indexIn != null) {
					indexIn.close();
					indexIn = null;
				}

				// generate index file
				indexFilePath = new Path(ExternalBlockShuffleUtils.generateIndexPath(fileInfo.getPartitionDir(), idx));
				// check whether index files exist
				if (!fileSystem.exists(indexFilePath)) {
					throw new IOException("Index file doesn't exist, file path: " + indexFilePath.getPath());
				}

				// open index file
				indexIn = fileSystem.open(indexFilePath);
				if (indexIn == null) {
					throw new IOException("cannot open index file, file path: " + indexFilePath.getPath());
				}
				indexView = new DataInputViewStreamWrapper(indexIn);

				// generate data file path for sharing among indices
				if (externalFileType != PersistentFileType.HASH_PARTITION_FILE) {
					// Unlike the other mode, data files in SINGLE_SUBPARTITION_FILE mode
					// is one per subpartition, we will generate its data files during
					// matrix transposition.
					dataFiles[idx] = new Path(ExternalBlockShuffleUtils.generateDataPath(fileInfo.getPartitionDir(), idx));
				}

				// load partition indices for the current spill
				PartitionIndex[] tmpPartitionIndexArr = ExternalBlockShuffleUtils.deserializeIndices(indexView, subpartitionNum);
				if (null == tmpPartitionIndexArr) {
					throw new IOException("cannot read index file, file path: " + indexFilePath.getPath());
				}
				partitionIndices[idx] = tmpPartitionIndexArr;
			}

		} finally {
			if (indexIn != null) {
				indexIn.close();
			}
		}

		// matrix transposition from partitionIndices to subpartitionMetas
		subpartitionMetas = (List<ExternalSubpartitionMeta>[])new ArrayList<?>[subpartitionNum];
		for (int subpartitionIndex = 0; subpartitionIndex < subpartitionNum; subpartitionIndex++) {
			List<ExternalSubpartitionMeta> subpartitionMeta = new ArrayList<>();
			for(int spillIdx = 0; spillIdx < spillCount; spillIdx++) {
				PartitionIndex partitionIndex = partitionIndices[spillIdx][subpartitionIndex];
				if (partitionIndex == null) {
					// if the subpartition has no data in this spill, index will be null
					continue;
				}
				Path dataFile;
				if (externalFileType == PersistentFileType.HASH_PARTITION_FILE) {
					// spillCount should be only one in this mode, so actual
					// new operation will do only once for each subpartition
					dataFile = new Path(ExternalBlockShuffleUtils.generateDataPath(fileInfo.getPartitionDir(), subpartitionIndex));
				} else {
					dataFile = dataFiles[spillIdx];
				}
				subpartitionMeta.add(new ExternalSubpartitionMeta(
					dataFile, partitionIndex.getStartOffset(), partitionIndex.getLength()));
			}
			subpartitionMetas[subpartitionIndex] = subpartitionMeta;
		}
	}

	// Utility for debug.
	private static String convertSubpartitionMetasToString(
		List<ExternalSubpartitionMeta> subpartitionMeta) {

		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("subpartition metas' detail: {");
		for (ExternalSubpartitionMeta meta : subpartitionMeta) {
			stringBuilder.append(meta.toString());
		}
		stringBuilder.append("}");
		return stringBuilder.toString();
	}

	static final class ExternalSubpartitionMeta {

		private final Path dataFile;

		private final long offset;

		private final long length;

		ExternalSubpartitionMeta(
			Path dataFile, long offset, long length) {

			assert dataFile != null;
			assert offset >= 0;
			assert length >= 0;

			this.dataFile = dataFile;
			this.offset = offset;
			this.length = length;
		}

		Path getDataFile() {
			return dataFile;
		}

		long getOffset() {
			return offset;
		}

		long getLength() {
			return length;
		}

		@Override
		public String toString() {
			return "{ dataFilePath = " + dataFile + ", offset = " + offset
				+ ", buffNum = " + length + " }";
		}
	}
}
