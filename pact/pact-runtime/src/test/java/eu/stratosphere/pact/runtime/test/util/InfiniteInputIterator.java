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

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * A simple iterator that returns an infinite amount of records resembling (0, 0) pairs.
 */
public class InfiniteInputIterator implements MutableObjectIterator<PactRecord>
{
	private final PactInteger val1 = new PactInteger(0);
	private final PactInteger val2 = new PactInteger(0);
	
	@Override
	public boolean next(PactRecord target) {
		target.setField(0, val1);
		target.setField(0, val2);
		return true;
	}
}
