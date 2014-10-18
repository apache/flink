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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;

import org.apache.flink.api.java.typeutils.runtime.tuple.base.TupleComparatorTestBase;

public class TupleComparatorISD1Test extends TupleComparatorTestBase<Tuple3<Integer, String, Double>> {

	@SuppressWarnings("unchecked")
	Tuple3<Integer, String, Double>[] dataISD = new Tuple3[]{
		new Tuple3<Integer, String, Double>(4, "hello", 20.0),
		new Tuple3<Integer, String, Double>(5, "hello", 23.2),
		new Tuple3<Integer, String, Double>(6, "world", 20.0),
		new Tuple3<Integer, String, Double>(7, "hello", 20.0),
		new Tuple3<Integer, String, Double>(8, "hello", 23.2),
		new Tuple3<Integer, String, Double>(9, "world", 20.0),
		new Tuple3<Integer, String, Double>(10, "hello", 20.0),
		new Tuple3<Integer, String, Double>(11, "hello", 23.2)
	};

	@Override
	protected TupleComparator<Tuple3<Integer, String, Double>> createComparator(boolean ascending) {
		return new TupleComparator<Tuple3<Integer, String, Double>>(
				new int[]{0},
				new TypeComparator[]{ new IntComparator(ascending) },
				new TypeSerializer[]{ IntSerializer.INSTANCE });
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TupleSerializer<Tuple3<Integer, String, Double>> createSerializer() {
		return new TupleSerializer<Tuple3<Integer, String, Double>>(
				(Class<Tuple3<Integer, String, Double>>) (Class<?>) Tuple3.class,
				new TypeSerializer[]{
					new IntSerializer(),
					new StringSerializer(),
					new DoubleSerializer()});
	}

	@Override
	protected Tuple3<Integer, String, Double>[] getSortedTestData() {
		return dataISD;
	}

}
