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
import eu.stratosphere.api.common.typeutils.base.DoubleComparator;
import eu.stratosphere.api.common.typeutils.base.DoubleSerializer;

public class DoubleComparatorTest extends ComparatorTestBase<Double> {

	@Override
	protected TypeComparator<Double> createComparator(boolean ascending) {
		return new DoubleComparator(ascending);
	}

	@Override
	protected TypeSerializer<Double> createSerializer() {
		return new DoubleSerializer();
	}
	
	@Override
	protected Double[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		double rndDouble = rnd.nextDouble();
		if (rndDouble < 0) {
			rndDouble = -rndDouble;
		}
		if (rndDouble == Double.MAX_VALUE) {
			rndDouble -= 3;
		}
		if (rndDouble <= 2) {
			rndDouble += 3;
		}
		return new Double[]{
			new Double(-rndDouble),
			new Double(-1.0D),
			new Double(0.0D),
			new Double(2.0D),
			new Double(rndDouble),
			new Double(Double.MAX_VALUE)};
	}
}
