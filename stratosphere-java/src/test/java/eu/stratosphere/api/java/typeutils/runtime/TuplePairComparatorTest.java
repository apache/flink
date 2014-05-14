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
package eu.stratosphere.api.java.typeutils.runtime;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.base.DoubleComparator;
import eu.stratosphere.api.common.typeutils.base.IntComparator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.runtime.tuple.base.TuplePairComparatorTestBase;

public class TuplePairComparatorTest extends TuplePairComparatorTestBase<Tuple3<Integer, String, Double>, Tuple3<Integer, Double, Long>> {

	@SuppressWarnings("unchecked")
	Tuple3<Integer, String, Double>[] dataISD = new Tuple3[]{
		new Tuple3<Integer, String, Double>(4, "hello", 20.0),
		new Tuple3<Integer, String, Double>(4, "world", 23.2),
		new Tuple3<Integer, String, Double>(5, "hello", 20.0),
		new Tuple3<Integer, String, Double>(5, "world", 23.2),
		new Tuple3<Integer, String, Double>(6, "hello", 20.0),
		new Tuple3<Integer, String, Double>(6, "world", 23.2),
		new Tuple3<Integer, String, Double>(7, "hello", 20.0),
		new Tuple3<Integer, String, Double>(7, "world", 23.2)
	};

	@SuppressWarnings("unchecked")
	Tuple3<Integer, Double, Long>[] dataIDL = new Tuple3[]{
		new Tuple3<Integer, Double, Long>(4, 20.0, new Long(14)),
		new Tuple3<Integer, Double, Long>(4, 23.2, new Long(15)),
		new Tuple3<Integer, Double, Long>(5, 20.0, new Long(15)),
		new Tuple3<Integer, Double, Long>(5, 23.2, new Long(20)),
		new Tuple3<Integer, Double, Long>(6, 20.0, new Long(20)),
		new Tuple3<Integer, Double, Long>(6, 23.2, new Long(29)),
		new Tuple3<Integer, Double, Long>(7, 20.0, new Long(29)),
		new Tuple3<Integer, Double, Long>(7, 23.2, new Long(34))
	};

	@Override
	protected TuplePairComparator<Tuple3<Integer, String, Double>, Tuple3<Integer, Double, Long>> createComparator(boolean ascending) {
		return new TuplePairComparator<Tuple3<Integer, String, Double>, Tuple3<Integer, Double, Long>>(
				new int[]{0, 2},
				new int[]{0, 1},
				new TypeComparator[]{
					new IntComparator(ascending),
					new DoubleComparator(ascending)
				},
				new TypeComparator[]{
					new IntComparator(ascending),
					new DoubleComparator(ascending)
				}
		);
	}

	@Override
	protected Tuple2<Tuple3<Integer, String, Double>[], Tuple3<Integer, Double, Long>[]> getSortedTestData() {
		return new Tuple2(dataISD, dataIDL);
	}
}
