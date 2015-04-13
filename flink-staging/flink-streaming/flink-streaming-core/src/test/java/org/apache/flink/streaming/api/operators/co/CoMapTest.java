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

package org.apache.flink.streaming.api.operators.co;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.util.MockCoContext;
import org.junit.Test;

public class CoMapTest implements Serializable {
	private static final long serialVersionUID = 1L;

	private final static class MyCoMap implements CoMapFunction<Double, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(Double value) {
			return value.toString();
		}

		@Override
		public String map2(Integer value) {
			return value.toString();
		}
	}

	@Test
	public void coMapTest() {
		CoStreamMap<Double, Integer, String> invokable = new CoStreamMap<Double, Integer, String>(new MyCoMap());

		List<String> expectedList = Arrays.asList("1.1", "1", "1.2", "2", "1.3", "3", "1.4", "1.5");
		List<String> actualList = MockCoContext.createAndExecute(invokable, Arrays.asList(1.1, 1.2, 1.3, 1.4, 1.5), Arrays.asList(1, 2, 3));
		
		assertEquals(expectedList, actualList);
	}
}
