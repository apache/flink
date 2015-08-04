/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// This only tests a fraction of FieldAccessor. The other parts are tested indirectly by AggregationFunctionTest.
public class FieldAccessorTest {

	@Test
	@SuppressWarnings("unchecked")
	public void arrayFieldAccessorTest() {
		int[] a = new int[]{3,5};
		FieldAccessor<int[], Integer> fieldAccessor =
				(FieldAccessor<int[], Integer>) (Object)
						FieldAccessor.create(1, PrimitiveArrayTypeInfo.getInfoFor(a.getClass()), null);

		assertEquals(Integer.class, fieldAccessor.getFieldType().getTypeClass());

		assertEquals((Integer)a[1], fieldAccessor.get(a));

		a = fieldAccessor.set(a, 6);
		assertEquals((Integer)a[1], fieldAccessor.get(a));



		Integer[] b = new Integer[]{3,5};
		FieldAccessor<Integer[], Integer> fieldAccessor2 =
				(FieldAccessor<Integer[], Integer>) (Object)
						FieldAccessor.create(1, BasicArrayTypeInfo.getInfoFor(b.getClass()), null);

		assertEquals(Integer.class, fieldAccessor2.getFieldType().getTypeClass());

		assertEquals((Integer)b[1], fieldAccessor2.get(b));

		b = fieldAccessor2.set(b, 6);
		assertEquals((Integer)b[1], fieldAccessor2.get(b));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tupleFieldAccessorOutOfBoundsTest() {
		try {
			FieldAccessor<Tuple2<Integer, Integer>, Integer> fieldAccessor =
					(FieldAccessor<Tuple2<Integer, Integer>, Integer>) (Object)
							FieldAccessor.create(2, TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class),
									null);
			fail();
		} catch (IndexOutOfBoundsException e) {
			// Nothing to do here
		}
	}
}
