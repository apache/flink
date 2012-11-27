/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.testing;

import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * Computes the difference between double values and returns {@link FuzzyTestValueSimilarity#NO_MATCH} if the difference
 * is
 * above a given threshold.
 * 
 * @author Arvid Heise
 */
public class DoubleValueSimilarity extends AbstractValueSimilarity<PactDouble> {
	private double delta;

	/**
	 * Initializes DoubleValueSimilarity with the given threshold.
	 * 
	 * @param delta
	 *        the threshold defining the maximum allowed difference.
	 */
	public DoubleValueSimilarity(double delta) {
		this.delta = delta;
	}

	/**
	 * Returns the threshold.
	 * 
	 * @return the threshold
	 */
	public double getDelta() {
		return this.delta;
	}

	@Override
	public double getDistance(PactDouble value1, PactDouble value2) {
		double diff = Math.abs(value1.getValue() - value2.getValue());
		if (diff <= this.delta)
			return diff;
		return NO_MATCH;
	}
}