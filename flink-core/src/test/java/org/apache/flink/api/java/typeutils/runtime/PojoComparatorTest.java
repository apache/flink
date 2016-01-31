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

import java.util.Arrays;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.Assert;


public class PojoComparatorTest extends ComparatorTestBase<PojoContainingTuple> {
	TypeInformation<PojoContainingTuple> type = TypeExtractor.getForClass(PojoContainingTuple.class);
	
	PojoContainingTuple[] data = new PojoContainingTuple[]{
		new PojoContainingTuple(1, 1L, 1L),
		new PojoContainingTuple(2, 2L, 2L),
		new PojoContainingTuple(8519, 85190L, 85190L),
		new PojoContainingTuple(8520, 85191L, 85191L),
	};

	@Override
	protected TypeComparator<PojoContainingTuple> createComparator(boolean ascending) {
		Assert.assertTrue(type instanceof CompositeType);
		CompositeType<PojoContainingTuple> cType = (CompositeType<PojoContainingTuple>) type;
		ExpressionKeys<PojoContainingTuple> keys = new ExpressionKeys<PojoContainingTuple>(new String[] {"theTuple.*"}, cType);
		boolean[] orders = new boolean[keys.getNumberOfKeyFields()];
		Arrays.fill(orders, ascending);
		return cType.createComparator(keys.computeLogicalKeyPositions(), orders, 0, new ExecutionConfig());
	}

	@Override
	protected TypeSerializer<PojoContainingTuple> createSerializer() {
		return type.createSerializer(new ExecutionConfig());
	}

	@Override
	protected PojoContainingTuple[] getSortedTestData() {
		return data;
	}
}
