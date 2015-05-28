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

package org.apache.flink.streaming.api.windowing.policy;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.junit.Test;

import java.util.List;
import java.util.Arrays;

import static org.junit.Assert.*;

public class DeltaPolicyTest {

	//Dummy serializer, this is not used because the tests are done locally
	private final static TypeSerializer<Tuple2<Integer, Integer>> SERIALIZER = null;

	@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
	@Test
	public void testDelta() {
		DeltaPolicy deltaPolicy = new DeltaPolicy(new DeltaFunction<Tuple2<Integer, Integer>>() {
			@Override
			public double getDelta(Tuple2<Integer, Integer> oldDataPoint,
					Tuple2<Integer, Integer> newDataPoint) {
				return (double) newDataPoint.f0 - oldDataPoint.f0;
			}
		}, new Tuple2(0, 0), 2, SERIALIZER);

		List<Tuple2> tuples = Arrays.asList(new Tuple2(1, 0), new Tuple2(2, 0), new Tuple2(3, 0),
				new Tuple2(6, 0));

		assertFalse(deltaPolicy.notifyTrigger(tuples.get(0)));
		assertEquals(0, deltaPolicy.notifyEviction(tuples.get(0), false, 0));

		assertFalse(deltaPolicy.notifyTrigger(tuples.get(1)));
		assertEquals(0, deltaPolicy.notifyEviction(tuples.get(1), false, 1));

		assertTrue(deltaPolicy.notifyTrigger(tuples.get(2)));
		assertEquals(1, deltaPolicy.notifyEviction(tuples.get(2), true, 2));

		assertTrue(deltaPolicy.notifyTrigger(tuples.get(3)));
		assertEquals(2, deltaPolicy.notifyEviction(tuples.get(3), true, 2));
	}

	@Test
	public void testEquality() {

		DeltaFunction<Tuple2<Integer, Integer>> df = new DeltaFunction<Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public double getDelta(Tuple2<Integer, Integer> oldDataPoint,
					Tuple2<Integer, Integer> newDataPoint) {
				return (double) newDataPoint.f0 - oldDataPoint.f0;
			}
		};

		assertEquals(new DeltaPolicy<Tuple2<Integer, Integer>>(df, new Tuple2<Integer, Integer>(0,
				0), 2, SERIALIZER), new DeltaPolicy<Tuple2<Integer, Integer>>(df, new Tuple2<Integer, Integer>(
				0, 0), 2, SERIALIZER));

		assertNotEquals(new DeltaPolicy<Tuple2<Integer, Integer>>(df, new Tuple2<Integer, Integer>(
				0, 1), 2, SERIALIZER), new DeltaPolicy<Tuple2<Integer, Integer>>(df,
				new Tuple2<Integer, Integer>(0, 0), 2, SERIALIZER));

		assertNotEquals(new DeltaPolicy<Tuple2<Integer, Integer>>(df, new Tuple2<Integer, Integer>(0,
				0), 2, SERIALIZER), new DeltaPolicy<Tuple2<Integer, Integer>>(df, new Tuple2<Integer, Integer>(
				0, 0), 3, SERIALIZER));

	}
}