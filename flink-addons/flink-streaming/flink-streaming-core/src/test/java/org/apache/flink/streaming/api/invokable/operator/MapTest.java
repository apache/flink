/**
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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.util.MockInvokable;
import org.junit.Test;

public class MapTest {

	private static class Map implements MapFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map(Integer value) throws Exception {
			return "+" + (value + 1);
		}
	}
	
	@Test
	public void mapInvokableTest() {
		MapInvokable<Integer, String> invokable = new MapInvokable<Integer, String>(new Map());
		
		List<String> expectedList = Arrays.asList("+2", "+3", "+4");
		List<String> actualList = MockInvokable.createAndExecute(invokable, Arrays.asList(1, 2, 3));
		
		assertEquals(expectedList, actualList);
	}
}
