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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * The default merge policy which supports both sync and async merging. When merging synchronously,
 * no file will be merged before the starting of final merge.
 */
public class DefaultFileMergePolicy<T> implements MergePolicy<T> {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileMergePolicy.class);

	/** Merge factor which indicates the max file handles per merge and the max number of final files. */
	private final int mergeFactor;

	/** Whether all files should be merged to one file at last. */
	private final boolean mergeToOneFile;

	/** Whether to merge asynchronously or not. */
	private final boolean enableAsyncMerging;

	/** Stores the merging candidates, and layer n stores files that has been merged for n round(s). */
	private List<LinkedList<DataFileInfo<T>>> layeredDataFiles = new ArrayList<>();

	/** Identifies whether the final merging has been started. */
	private boolean isFinalMergeStarted = false;

	public DefaultFileMergePolicy(int mergeFactor, boolean enableAsyncMerging, boolean mergeToOneFile) {
		Preconditions.checkArgument(mergeFactor >= 2, "Illegal merge factor: " + mergeFactor);

		this.mergeFactor = mergeFactor;
		this.enableAsyncMerging = enableAsyncMerging;
		this.mergeToOneFile = mergeToOneFile;
	}

	@Override
	public void addNewCandidate(DataFileInfo<T> dataFileInfo) {
		if (isFinalMergeStarted) {
			// after starting final merger, there should be only one layer of candidate files
			Preconditions.checkState(layeredDataFiles.size() == 1,
				"Illegal layer size: " + layeredDataFiles.size());
			layeredDataFiles.get(0).addLast(dataFileInfo);
		} else {
			int mergeRound = dataFileInfo.getMergeRound();
			Preconditions.checkArgument(layeredDataFiles.size() >= mergeRound,
				"Illegal merge round: (" + mergeRound + " " + layeredDataFiles.size() + ")");
			if (layeredDataFiles.size() == mergeRound) {
				LinkedList<DataFileInfo<T>> dataFiles = new LinkedList<>();
				layeredDataFiles.add(dataFiles);
			}
			layeredDataFiles.get(mergeRound).addLast(dataFileInfo);
		}
	}

	@Override
	public void startFinalMerge() {
		Preconditions.checkState(!isFinalMergeStarted, "Final merge has been started");
		isFinalMergeStarted = true;
		// merge all remaining data files to one list
		for (int i = 1; i < layeredDataFiles.size(); ++i) {
			layeredDataFiles.get(0).addAll(layeredDataFiles.get(i));
		}
		if (layeredDataFiles.size() > 1) {
			layeredDataFiles = layeredDataFiles.subList(0, 1);
		}
	}

	@Override
	public List<DataFileInfo<T>> selectMergeCandidates(int numMergeReadMemory) {
		int numFileHandles = Math.min(mergeFactor, numMergeReadMemory / 2);
		ArrayList<DataFileInfo<T>> candidates = new ArrayList<>(numFileHandles);
		if (layeredDataFiles.isEmpty()) {
			// this can happen when no spilled file is added and the final merge has been started
			Preconditions.checkState(isFinalMergeStarted, "Final merge should have been started.");
			return null;
		} else if (isFinalMergeStarted) {
			LinkedList<DataFileInfo<T>> dataFiles = layeredDataFiles.get(0);
			Preconditions.checkArgument(numMergeReadMemory >= 4 || dataFiles.size() <= 1,
				"At least 4 read buffers is needed, but actual is " + numMergeReadMemory);
			// sort the file list so the smaller files can be picked out and got merged
			dataFiles.sort(new FileLengthComparator<>());
			if (dataFiles.size() > mergeFactor && !mergeToOneFile) {
				// merge as few files as possible
				numFileHandles = Math.min(numFileHandles, dataFiles.size() - mergeFactor + 1);
			} else if (mergeToOneFile && dataFiles.size() > 1) {
				if (dataFiles.size() <= numFileHandles) {
					// can be merged in one round
					numFileHandles = dataFiles.size();
				} else {
					// merge as few files as possible
					numFileHandles = Math.min(numFileHandles, dataFiles.size() - numFileHandles + 1);
				}
			} else {
				// there is no need to merge
				return null;
			}

			// remove and return the chosen candidates
			for (int i = 0; i < numFileHandles; ++i) {
				candidates.add(dataFiles.removeFirst());
			}
			return candidates;
		} else if (enableAsyncMerging) {
			Preconditions.checkArgument(numMergeReadMemory >= 4,
				"At least 4 read buffers is needed, but actual is " + numMergeReadMemory);
			for (LinkedList<DataFileInfo<T>> dataFiles: layeredDataFiles) {
				if (dataFiles.size() > mergeFactor) {
					for (int i = 0; i < numFileHandles; ++i) {
						candidates.add(dataFiles.removeFirst());
					}
					return candidates;
				}
			}
		}
		return null;
	}

	@Override
	public List<T> getFinalMergeResult() {
		Preconditions.checkState(layeredDataFiles.size() <= 1, "Illegal merge state: " + layeredDataFiles.size());
		ArrayList<T> dataFiles = new ArrayList<>();
		if (layeredDataFiles.size() > 0) {
			for (DataFileInfo<T> fileInfo : layeredDataFiles.get(0)) {
				dataFiles.add(fileInfo.getDataFile());
			}
		}
		return dataFiles;
	}

	/**
	 * The comparator used to sort the file candidates in ascending order of file length.
	 */
	private static class FileLengthComparator<T> implements Comparator<DataFileInfo<T>> {
		@Override
		public int compare(DataFileInfo<T> file1, DataFileInfo<T> file2) {
			if (file1.getFileLength() == file2.getFileLength()) {
				return 0;
			} else {
				return file1.getFileLength() >= file2.getFileLength() ? 1 : -1;
			}
		}
	}
}
