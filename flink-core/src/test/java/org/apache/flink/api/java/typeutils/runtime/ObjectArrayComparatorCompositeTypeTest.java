/*
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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Assert;

import java.lang.reflect.Array;

public class ObjectArrayComparatorCompositeTypeTest extends ComparatorTestBase<Tuple2<String, Integer>[][]> {
	private final TypeInformation<Tuple2<String, Integer>[]> componentInfo;

	public ObjectArrayComparatorCompositeTypeTest() {
		this.componentInfo = ObjectArrayTypeInfo.getInfoFor(new TupleTypeInfo<Tuple>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeSerializer<Tuple2<String, Integer>[][]> createSerializer() {
		return (TypeSerializer<Tuple2<String, Integer>[][]>) new GenericArraySerializer<Tuple2<String, Integer>[]>(
			componentInfo.getTypeClass(),
			componentInfo.createSerializer(null));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeComparator<Tuple2<String, Integer>[][]> createComparator(boolean ascending) {
		CompositeType<? extends Object> baseComponentInfo = new TupleTypeInfo<Tuple>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		int componentArity = baseComponentInfo.getArity();
		int [] logicalKeyFields = new int[componentArity];
		boolean[] orders = new boolean[componentArity];

		for (int i=0;i < componentArity;i++) {
			logicalKeyFields[i] = i;
			orders[i] = ascending;
		}

		return (TypeComparator<Tuple2<String, Integer>[][]>) new ObjectArrayComparator<Tuple2<String, Integer>[], Character>(ascending,
			(GenericArraySerializer<Tuple2<String, Integer>[]>) createSerializer(),
			((CompositeType<? super Object>) baseComponentInfo).createComparator(logicalKeyFields, orders, 0, null)
		);
	}

	@Override
	protected void deepEquals(String message, Tuple2<String, Integer>[][] should, Tuple2<String, Integer>[][] is) {
		Assert.assertTrue(should.length==is.length);
		for (int i=0;i < should.length;i++) {
			Assert.assertTrue(should[i].length==is[i].length);
			for (int j=0;j < should[i].length;j++) {
				Assert.assertEquals(should[i][j].f0,is[i][j].f0);
				Assert.assertEquals(should[i][j].f1,is[i][j].f1);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Tuple2<String, Integer>[][][] getSortedTestData() {
		Object result = Array.newInstance(Tuple2.class, new int[]{2, 2, 1});

		((Tuple2<String, Integer>[][][]) result)[0][0][0] = new Tuple2<String, Integer>();
		((Tuple2<String, Integer>[][][]) result)[0][0][0].f0 = "be";
		((Tuple2<String, Integer>[][][]) result)[0][0][0].f1 = 2;

		((Tuple2<String, Integer>[][][]) result)[0][1][0] = new Tuple2<String, Integer>();
		((Tuple2<String, Integer>[][][]) result)[0][1][0].f0 = "not";
		((Tuple2<String, Integer>[][][]) result)[0][1][0].f1 = 3;


		((Tuple2<String, Integer>[][][]) result)[1][0][0] = new Tuple2<String, Integer>();
		((Tuple2<String, Integer>[][][]) result)[1][0][0].f0 = "or";
		((Tuple2<String, Integer>[][][]) result)[1][0][0].f1 = 2;

		((Tuple2<String, Integer>[][][]) result)[1][1][0] = new Tuple2<String, Integer>();
		((Tuple2<String, Integer>[][][]) result)[1][1][0].f0 = "to";
		((Tuple2<String, Integer>[][][]) result)[1][1][0].f1 = 2;

		return (Tuple2<String, Integer>[][][]) result;
	}
}
