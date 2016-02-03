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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

public class TupleTypeInfoTest extends TestLogger {

	@Test
	public void testTupleTypeInfoSymmetricEqualityRelation() {
		TupleTypeInfo<Tuple1<Integer>> tupleTypeInfo = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO);

		TupleTypeInfoBase<Tuple1> anonymousTupleTypeInfo = new TupleTypeInfoBase<Tuple1>(
			(Class<Tuple1>)Tuple1.class,
			(TypeInformation<?>)BasicTypeInfo.INT_TYPE_INFO) {

			private static final long serialVersionUID = -7985593598027660836L;

			@Override
			public TypeSerializer<Tuple1> createSerializer(ExecutionConfig config) {
				return null;
			}

			@Override
			protected TypeComparatorBuilder<Tuple1> createTypeComparatorBuilder() {
				return null;
			}

			@Override
			public String[] getFieldNames() {
				return new String[0];
			}

			@Override
			public int getFieldIndex(String fieldName) {
				return 0;
			}
		};

		boolean tupleVsAnonymous = tupleTypeInfo.equals(anonymousTupleTypeInfo);
		boolean anonymousVsTuple = anonymousTupleTypeInfo.equals(tupleTypeInfo);

		Assert.assertTrue("Equality relation should be symmetric", tupleVsAnonymous == anonymousVsTuple);
	}

	@Test
	public void testTupleTypeInfoEquality() {
		TupleTypeInfo<Tuple2<Integer, String>> tupleTypeInfo1 = new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		TupleTypeInfo<Tuple2<Integer, String>> tupleTypeInfo2 = new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		Assert.assertEquals(tupleTypeInfo1, tupleTypeInfo2);
		Assert.assertEquals(tupleTypeInfo1.hashCode(), tupleTypeInfo2.hashCode());
	}

	@Test
	public void testTupleTypeInfoInequality() {
		TupleTypeInfo<Tuple2<Integer, String>> tupleTypeInfo1 = new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		TupleTypeInfo<Tuple2<Integer, Boolean>> tupleTypeInfo2 = new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.BOOLEAN_TYPE_INFO);

		Assert.assertNotEquals(tupleTypeInfo1, tupleTypeInfo2);
	}
}
