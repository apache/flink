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

/**
 * This encapsulates the basic information of a data file to be used by the merge policy. Different
 * merge policies may depend on different information.
 */
public class DataFileInfo<T> {

	/**
	 * Indicates how many times of merging before this file is produced. The initial merge round of
	 * the spilled files is 0, and after each merge, the new merge round equals to the maximum merge
	 * round of the merge source files plus 1.
	 */
	private final int mergeRound;

	private final long fileLength;
	private final int numSubpartitions;
	private final T dataFile;

	public DataFileInfo(long fileLength,
						int mergeRound,
						int numSubpartitions,
						T dataFile) {
		Preconditions.checkArgument(fileLength > 0, "Illegal file length: " + fileLength);
		Preconditions.checkArgument(mergeRound >= 0, "Illegal merge round: " + mergeRound);
		Preconditions.checkArgument(numSubpartitions > 0, "Illegal number of subpartitions: " + numSubpartitions);

		this.fileLength = fileLength;
		this.mergeRound = mergeRound;
		this.numSubpartitions = numSubpartitions;
		this.dataFile = Preconditions.checkNotNull(dataFile);
	}

	public long getFileLength() {
		return fileLength;
	}

	public int getMergeRound() {
		return mergeRound;
	}

	public int getNumSubpartitions() {
		return numSubpartitions;
	}

	public T getDataFile() {
		return dataFile;
	}
}
