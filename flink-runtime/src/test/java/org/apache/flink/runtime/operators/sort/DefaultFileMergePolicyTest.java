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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultFileMergePolicy}.
 */
@RunWith(Parameterized.class)
public class DefaultFileMergePolicyTest {

	private final int numFiles;

	private final int mergeFactor;

	private final int numMemSegs;

	private final boolean async;

	private final boolean mergeToOneFile;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{0, 2, 100, false, false},
			{0, 2, 100, false, true},
			{0, 2, 100, true, false},
			{0, 2, 100, true, true},

			{10, 2, 100, false, false},
			{10, 2, 100, false, true},
			{10, 2, 100, true, false},
			{10, 2, 100, true, true},

			{100, 2, 100, false, false},
			{100, 2, 100, false, true},
			{100, 2, 100, true, false},
			{100, 2, 100, true, true},

			{1000, 2, 100, false, false},
			{1000, 2, 100, false, true},
			{1000, 2, 100, true, false},
			{1000, 2, 100, true, true},

			{10000, 2, 100, false, false},
			{10000, 2, 100, false, true},
			{10000, 2, 100, true, false},
			{10000, 2, 100, true, true},

			{20000, 2, 100, false, false},
			{20000, 2, 100, false, true},
			{20000, 2, 100, true, false},
			{20000, 2, 100, true, true},

			{0, 100, 100, false, false},
			{0, 100, 100, false, true},
			{0, 100, 100, true, false},
			{0, 100, 100, true, true},

			{10, 100, 100, false, false},
			{10, 100, 100, false, true},
			{10, 100, 100, true, false},
			{10, 100, 100, true, true},

			{100, 100, 100, false, false},
			{100, 100, 100, false, true},
			{100, 100, 100, true, false},
			{100, 100, 100, true, true},

			{1000, 100, 100, false, false},
			{1000, 100, 100, false, true},
			{1000, 100, 100, true, false},
			{1000, 100, 100, true, true},

			{10000, 100, 100, false, false},
			{10000, 100, 100, false, true},
			{10000, 100, 100, true, false},
			{10000, 100, 100, true, true},

			{100000, 100, 100, false, false},
			{100000, 100, 100, false, true},
			{100000, 100, 100, true, false},
			{100000, 100, 100, true, true},
		});
	}

	public DefaultFileMergePolicyTest(int numFiles, int mergeFactor, int numMemSegs, boolean async, boolean mergeToOneFile) {
		this.numFiles = numFiles;
		this.mergeFactor = mergeFactor;
		this.numMemSegs = numMemSegs;
		this.async = async;
		this.mergeToOneFile = mergeToOneFile;
	}

	@Test
	public void test() {
		MergePolicy<Long> mergePolicy = new DefaultFileMergePolicy<>(mergeFactor, async, mergeToOneFile);

		for (int i = 0; i < numFiles; ++i) {
			DataFileInfo<Long> fileInfo = new DataFileInfo<>(100, 0, 10, new Long(100));
			mergePolicy.addNewCandidate(fileInfo);
			List<DataFileInfo<Long>> candidates = mergePolicy.selectMergeCandidates(numMemSegs);

			if (!async) {
				assertTrue("No candidates should be selected.", candidates == null);
			}

			while(candidates != null) {
				assertTrue("Should select more than one file.", candidates.size() > 0);
				assertTrue("Should select Math.min(mergeFactor, numMemSegs / 2) number of files.", candidates.size() == Math.min(mergeFactor, numMemSegs / 2));
				int layer = candidates.get(0).getMergeRound();
				int newFileLength = 0;
				for (DataFileInfo<Long> info: candidates) {
					newFileLength += info.getFileLength();
					assertTrue("Candidates should be from the same layer.", info.getMergeRound() == layer);
				}
				fileInfo = new DataFileInfo<>(newFileLength, layer + 1, 10, new Long(newFileLength));
				mergePolicy.addNewCandidate(fileInfo);
				candidates = mergePolicy.selectMergeCandidates(numMemSegs);
			}
		}

		mergePolicy.startFinalMerge();
		List<DataFileInfo<Long>> candidates = mergePolicy.selectMergeCandidates(numMemSegs);
		boolean lastRoundOfMergeStart = false;
		while(candidates != null) {
			assertTrue("Should select more than one file.", candidates.size() > 0);
			if (candidates.size() != Math.min(mergeFactor, numMemSegs / 2) && !lastRoundOfMergeStart) {
				lastRoundOfMergeStart = true;
			} else if (candidates.size() != Math.min(mergeFactor, numMemSegs / 2)) {
				fail("Should select Math.min(mergeFactor, numMemSegs / 2) number of files.");
			}

			int newFileLength = 0;
			int maxLayer = 0;
			long fileLength = candidates.get(0).getFileLength();
			for (DataFileInfo<Long> info: candidates) {
				newFileLength += info.getFileLength();
				assertTrue("File length should be in ascending order.", fileLength <= info.getFileLength());
				fileLength = info.getFileLength();
				maxLayer = Math.max(maxLayer, info.getMergeRound());
			}
			DataFileInfo<Long> fileInfo = new DataFileInfo<>(newFileLength, maxLayer + 1, 10, new Long(newFileLength));
			mergePolicy.addNewCandidate(fileInfo);
			candidates = mergePolicy.selectMergeCandidates(numMemSegs);
		}

		List<Long> finalFileList = mergePolicy.getFinalMergeResult();
		if (numFiles == 0) {
			assertTrue("There should be no file.", finalFileList.isEmpty());
		} else if (mergeToOneFile) {
			assertTrue("Should be merged to one file.", finalFileList.size() == 1);
		} else {
			assertTrue("The number of result files should be less than merge factor.", finalFileList.size() <= mergeFactor);
		}

		long totalLength = 0;
		for (Long length: finalFileList) {
			totalLength += length;
		}
		assertTrue("Error total file length.", totalLength == 100 * numFiles);
	}
}
