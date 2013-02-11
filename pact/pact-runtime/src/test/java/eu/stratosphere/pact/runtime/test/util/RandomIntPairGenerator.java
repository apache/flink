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

package eu.stratosphere.pact.runtime.test.util;

import java.util.Random;

import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.test.util.types.IntPair;

public class RandomIntPairGenerator implements MutableObjectIterator<IntPair>
{
	private final long seed;
	
	private final long numRecords;
	
	private Random rnd;
	
	private long count;
	
	
	public RandomIntPairGenerator(long seed) {
		this(seed, Long.MAX_VALUE);
	}
	
	public RandomIntPairGenerator(long seed, long numRecords) {
		this.seed = seed;
		this.numRecords = numRecords;
		this.rnd = new Random(seed);
	}

	
	@Override
	public boolean next(IntPair target) {
		if (this.count++ < this.numRecords) {
			target.setKey(this.rnd.nextInt());
			target.setValue(this.rnd.nextInt());
			return true;
		} else {
			return false;
		}
	}
	
	public void reset() {
		this.rnd = new Random(this.seed);
		this.count = 0;
	}
}
