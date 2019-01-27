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

import java.util.List;

/**
 * The interface is defined to support both sync and async merging of data files.
 *
 * @param <T> type of the data file
 */
public interface MergePolicy<T> {

	/**
	 * The method is called when a new data file is created (produced spilling or merging).
	 * New data files will be produced continually both in sync and async merging mode, the
	 * merging policy will decide to merge asynchronously or synchronously based on the
	 * user-defined configuration.
	 *
	 * @param dataFileInfo  basic information of the new file, including the file itself.
	 */
	void addNewCandidate(DataFileInfo<T> dataFileInfo);

	/**
	 * The method is called after the finishing of spilling phase, after which the final
	 * merge can be performed. At this point different merge policy can be adopted, for
	 * example, all remaining files can be merged into one file.
	 */
	void startFinalMerge();

	/**
	 * Selects a list of candidates to be merge by the merger. The merger will call this
	 * method continually until the merge policy believes there is no need to merge any more.
	 *
	 * @param numMergeReadMemory the number of read memory segment can be used.
	 * @return the selected candidate files to be merged or null if there is no need to merge.
	 */
	List<DataFileInfo<T>> selectMergeCandidates(int numMergeReadMemory);

	/**
	 * Gets the final file list after the merging phase finishes.
	 *
	 * @return the final merged result.
	 */
	List<T> getFinalMergeResult();
}
