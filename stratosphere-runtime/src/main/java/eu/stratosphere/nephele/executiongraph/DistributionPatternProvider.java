/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.jobgraph.DistributionPattern;

public final class DistributionPatternProvider {

	/**
	 * Checks if two subtasks of different tasks should be wired.
	 * 
	 * @param pattern
	 *        the distribution pattern that should be used
	 * @param nodeLowerStage
	 *        the index of the producing task's subtask
	 * @param nodeUpperStage
	 *        the index of the consuming task's subtask
	 * @param sizeSetLowerStage
	 *        the number of subtasks of the producing task
	 * @param sizeSetUpperStage
	 *        the number of subtasks of the consuming task
	 * @return <code>true</code> if a wire between the two considered subtasks should be created, <code>false</code>
	 *         otherwise
	 */
	public static boolean createWire(final DistributionPattern pattern, final int nodeLowerStage,
			final int nodeUpperStage, final int sizeSetLowerStage, final int sizeSetUpperStage) {

		switch (pattern) {
		case BIPARTITE:
			return true;

		case POINTWISE:
			if (sizeSetLowerStage < sizeSetUpperStage) {
				if (nodeLowerStage == (nodeUpperStage % sizeSetLowerStage)) {
					return true;
				}
			} else {
				if ((nodeLowerStage % sizeSetUpperStage) == nodeUpperStage) {
					return true;
				}
			}

			return false;

			/*
			 * case STAR:
			 * if (sizeSetLowerStage > sizeSetUpperStage) {
			 * int groupNumber = nodeLowerStage / Math.max(sizeSetLowerStage / sizeSetUpperStage, 1);
			 * if (nodeUpperStage == groupNumber) {
			 * return true;
			 * }
			 * } else {
			 * int groupNumber = nodeUpperStage / Math.max(sizeSetUpperStage / sizeSetLowerStage, 1);
			 * if (nodeLowerStage == groupNumber) {
			 * return true;
			 * }
			 * }
			 * return false;
			 */

		default:
			// this will never happen.
			throw new IllegalStateException("No Match for Distribution Pattern found.");
		}
	}
}
