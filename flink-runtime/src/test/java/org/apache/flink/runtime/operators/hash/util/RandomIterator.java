/**
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


package org.apache.flink.runtime.operators.hash.util;

import java.util.Iterator;
import java.util.Random;

/**
 * Gives a number of random integer values
 * 
 * 
 */
public class RandomIterator implements Iterator<Integer> {

	private final Random randomGenerator;
	private final int count;
	private int currentCount;

	/**
	 * Creates an iterator which gives random integer numbers
	 * 
	 * @param seed
	 *            Seed for random generator
	 * @param count
	 *            Number of values to be returned
	 */
	public RandomIterator(long seed, int count) {
		this.randomGenerator = new Random(seed);
		this.count = count;
		this.currentCount = 0;
	}

	@Override
	public boolean hasNext() {
		return (currentCount < count);
	}

	@Override
	public Integer next() {
		currentCount++;
		return randomGenerator.nextInt();
	}

	@Override
	public void remove() {
	}

}
