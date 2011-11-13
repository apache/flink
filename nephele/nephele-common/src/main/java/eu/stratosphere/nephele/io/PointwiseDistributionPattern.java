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

package eu.stratosphere.nephele.io;

/**
 * Represents a point-wise distribution pattern, i.e. given two distinct equally-sized sets of vertices A and B, each
 * vertex of set A
 * is wired to exactly one vertex of set B. If one of the sets is larger than the other vertices from the smaller set
 * may receive more than
 * wire.
 * 
 * @author warneke
 */
public class PointwiseDistributionPattern implements DistributionPattern {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean createWire(final int nodeLowerStage, final int nodeUpperStage, final int sizeSetLowerStage,
			final int sizeSetUpperStage) {

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
	}

}
