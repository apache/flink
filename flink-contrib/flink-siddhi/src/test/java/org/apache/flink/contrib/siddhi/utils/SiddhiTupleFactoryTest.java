/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.utils;

import org.apache.flink.api.java.tuple.Tuple5;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SiddhiTupleFactoryTest {
	@Test
	public void testConvertObjectArrayToTuple() {
		Object[] row = new Object[]{1, "message", 1234567L, true, new Object()};
		Tuple5 tuple5 = SiddhiTupleFactory.newTuple(row);
		assertEquals(5, tuple5.getArity());
		assertArrayEquals(row, new Object[]{
			tuple5.f0,
			tuple5.f1,
			tuple5.f2,
			tuple5.f3,
			tuple5.f4
		});
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConvertTooLongObjectArrayToTuple() {
		Object[] row = new Object[26];
		SiddhiTupleFactory.newTuple(row);
	}
}
