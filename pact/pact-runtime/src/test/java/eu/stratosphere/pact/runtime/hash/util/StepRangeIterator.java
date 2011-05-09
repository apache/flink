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

package eu.stratosphere.pact.runtime.hash.util;

import java.util.Iterator;

/**
 * Iterates over a range of integer values with a certain step width
 * 
 * @author Matthias Ringwald
 * 
 */
public class StepRangeIterator implements Iterator<Integer> {

	private final int maxValue;
	private int currentValue;
	private final int step;

	/**
	 * Create an iterator over the range from minValue to maxValue with a
	 * certain step width
	 * 
	 * @param minValue
	 *            Smallest value returned by the iterator
	 * @param maxValue
	 *            Largest value returned by the iterator
	 * @param step
	 *            Step width of the iterator
	 */
	public StepRangeIterator(int minValue, int maxValue, int step) {
		this.maxValue = maxValue;
		currentValue = minValue;
		this.step = step;
	}

	@Override
	public boolean hasNext() {
		return (currentValue < maxValue);
	}

	@Override
	public Integer next() {
		int temp = currentValue;
		currentValue += step;
		return temp;
	}

	@Override
	public void remove() {
	}

}
