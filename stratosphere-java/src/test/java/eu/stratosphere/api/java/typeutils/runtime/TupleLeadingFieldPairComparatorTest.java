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

import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.base.IntComparator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.typeutils.runtime.tuple.base.TuplePairComparatorTestBase;

public class TupleLeadingFieldPairComparatorTest extends TuplePairComparatorTestBase<Tuple3<Integer, String, Double>, Tuple4<Integer, Float, Long, Double>> {

	@SuppressWarnings("unchecked")
	private Tuple3<Integer, String, Double>[] dataISD = new Tuple3[]{
		new Tuple3<Integer, String, Double>(4, "hello", 20.0),
		new Tuple3<Integer, String, Double>(5, "world", 23.2),
		new Tuple3<Integer, String, Double>(6, "hello", 18.0),
		new Tuple3<Integer, String, Double>(7, "world", 19.2),
		new Tuple3<Integer, String, Double>(8, "hello", 16.0),
		new Tuple3<Integer, String, Double>(9, "world", 17.2),
		new Tuple3<Integer, String, Double>(10, "hello", 14.0),
		new Tuple3<Integer, String, Double>(11, "world", 15.2)
	};

	@SuppressWarnings("unchecked")
	private Tuple4<Integer, Float, Long, Double>[] dataIDL = new Tuple4[]{
		new Tuple4<Integer, Float, Long, Double>(4, 0.11f, 14L, 20.0),
		new Tuple4<Integer, Float, Long, Double>(5, 0.221f, 15L, 23.2),
		new Tuple4<Integer, Float, Long, Double>(6, 0.33f, 15L, 18.0),
		new Tuple4<Integer, Float, Long, Double>(7, 0.44f, 20L, 19.2),
		new Tuple4<Integer, Float, Long, Double>(8, 0.55f, 20L, 16.0),
		new Tuple4<Integer, Float, Long, Double>(9, 0.66f, 29L, 17.2),
		new Tuple4<Integer, Float, Long, Double>(10, 0.77f, 29L, 14.0),
		new Tuple4<Integer, Float, Long, Double>(11, 0.88f, 34L, 15.2)
	};

	@Override
	protected TypePairComparator<Tuple3<Integer, String, Double>, Tuple4<Integer, Float, Long, Double>> createComparator(boolean ascending) {
		return new TupleLeadingFieldPairComparator<Integer, Tuple3<Integer, String, Double>, Tuple4<Integer, Float, Long, Double>>(
				new IntComparator(ascending), new IntComparator(ascending));
	}

	@Override
	protected Tuple2<Tuple3<Integer, String, Double>[], Tuple4<Integer, Float, Long, Double>[]> getSortedTestData() {
		return new Tuple2<Tuple3<Integer, String, Double>[], Tuple4<Integer, Float, Long, Double>[]>(dataISD, dataIDL);
	}
}
