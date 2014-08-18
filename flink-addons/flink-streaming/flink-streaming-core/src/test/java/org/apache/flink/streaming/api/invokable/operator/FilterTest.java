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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.util.MockInvokable;
import org.junit.Test;

public class FilterTest implements Serializable {
	private static final long serialVersionUID = 1L;

	static class MyFilter implements FilterFunction<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Integer value) throws Exception {
			return value % 2 == 0;
		}
	}

	@Test 
	public void test() {
		FilterInvokable<Integer> invokable = new FilterInvokable<Integer>(new MyFilter());

		List<Integer> expected = Arrays.asList(2, 4, 6);
		List<Integer> actual = MockInvokable.createAndExecute(invokable, Arrays.asList(1, 2, 3, 4, 5, 6, 7));
		
		assertEquals(expected, actual);
	}
}
