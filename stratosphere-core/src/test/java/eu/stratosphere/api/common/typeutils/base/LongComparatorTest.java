/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.common.typeutils.base;

import java.util.Random;

import eu.stratosphere.api.common.typeutils.ComparatorTestBase;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.LongComparator;
import eu.stratosphere.api.common.typeutils.base.LongSerializer;

public class LongComparatorTest extends ComparatorTestBase<Long> {

	@Override
	protected TypeComparator<Long> createComparator(boolean ascending) {
		return new LongComparator(ascending);
	}

	@Override
	protected TypeSerializer<Long> createSerializer() {
		return new LongSerializer();
	}

	@Override
	protected Long[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		long rndLong = rnd.nextLong();
		if (rndLong < 0) {
			rndLong = -rndLong;
		}
		if (rndLong == Long.MAX_VALUE) {
			rndLong -= 3;
		}
		if (rndLong <= 2) {
			rndLong += 3;
		}
		return new Long[]{
			new Long(Long.MIN_VALUE),
			new Long(-rndLong),
			new Long(-1L),
			new Long(0L),
			new Long(1L),
			new Long(2L),
			new Long(rndLong),
			new Long(Long.MAX_VALUE)};
	}
}
