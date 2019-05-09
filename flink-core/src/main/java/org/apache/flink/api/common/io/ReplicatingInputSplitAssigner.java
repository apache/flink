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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Assigns each InputSplit to each requesting parallel instance.
 * This causes the input to be fully replicated, i.e., each parallel instance consumes the full input.
 */
@Internal
public class ReplicatingInputSplitAssigner implements InputSplitAssigner {

	private InputSplit[] inputSplits;

	private int[] assignCounts;

	public ReplicatingInputSplitAssigner(Collection<InputSplit> splits) {
		this.inputSplits = new InputSplit[splits.size()];
		this.inputSplits = splits.toArray(this.inputSplits);
		this.assignCounts = new int[32];
		Arrays.fill(assignCounts, 0);
	}

	public ReplicatingInputSplitAssigner(InputSplit[] splits) {
		this.inputSplits = splits;
		this.assignCounts = new int[32];
		Arrays.fill(assignCounts, 0);
	}

	@Override
	public InputSplit getNextInputSplit(String host, int taskId) {

		// get assignment count
		Integer assignCnt;
		if(taskId < this.assignCounts.length) {
			assignCnt = this.assignCounts[taskId];
		} else {
			int newSize = this.assignCounts.length * 2;
			if (taskId >= newSize) {
				newSize = taskId;
			}
			int[] newAssignCounts = Arrays.copyOf(assignCounts, newSize);
			Arrays.fill(newAssignCounts, assignCounts.length, newSize, 0);

			assignCnt = 0;
		}

		if(assignCnt >= inputSplits.length) {
			// all splits for this task have been assigned
			return null;
		} else {
			// return next splits
			InputSplit is = inputSplits[assignCnt];
			assignCounts[taskId] = assignCnt+1;
			return is;
		}

	}

	@Override
	public void returnInputSplit(List<InputSplit> splits, int taskId) {
		Preconditions.checkArgument(taskId >=0 && taskId < assignCounts.length);
		assignCounts[taskId] = 0;
	}
}
