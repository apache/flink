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

import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.junit.Assert;

public class ObjectArrayComparatorPrimitiveTypeTest extends ComparatorTestBase<char[][]> {
	private final TypeInformation<char[]> componentInfo;

	public ObjectArrayComparatorPrimitiveTypeTest() {
		this.componentInfo = PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO;
	}

	@Override
	protected TypeSerializer<char[][]> createSerializer() {
		return (TypeSerializer<char[][]>) new GenericArraySerializer<char[]>(
			componentInfo.getTypeClass(),
			componentInfo.createSerializer(null));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeComparator<char[][]> createComparator(boolean ascending) {
		TypeInformation<Character> baseComponentInfo = BasicTypeInfo.CHAR_TYPE_INFO;
		return (TypeComparator<char[][]>) new ObjectArrayComparator<char[], Character>(ascending,
			(GenericArraySerializer<char[]>) createSerializer(),
			((AtomicType<? super Object>) baseComponentInfo).createComparator(ascending, null)
		);
	}

	@Override
	protected void deepEquals(String message, char[][] should, char[][] is) {
		Assert.assertTrue(should.length==is.length);
		for (int i=0; i < should.length;i++) {
			Assert.assertTrue(should[i].length==is[i].length);
			for (int j=0;j < should[i].length;j++) {
				Assert.assertEquals(should[i][j], is[i][j]);
			}
		}
	}

	@Override
	protected char[][][] getSortedTestData() {
		return new char[][][]{
			new char[][]{
				new char[]{'b', 'e'},
				new char[]{'2'},
			},
			new char[][]{
				new char[]{'n', 'o', 't'},
				new char[]{'3'}
			},
			new char[][]{
				new char[]{'o', 'r'},
				new char[]{'2'}
			},
			new char[][]{
				new char[]{'t', 'o'},
				new char[]{'2'}
			}
		};
	}
}
