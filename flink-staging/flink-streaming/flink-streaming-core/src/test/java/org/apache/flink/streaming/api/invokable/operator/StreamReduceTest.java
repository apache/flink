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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class StreamReduceTest {

	private static class MyReducer implements ReduceFunction<Integer>{

		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1+value2;
		}
		
	}
	
	@Test
	public void test() {
		StreamReduceInvokable<Integer> invokable1 = new StreamReduceInvokable<Integer>(
				new MyReducer());

		List<Integer> expected = Arrays.asList(1,2,4,7,10);
		List<Integer> actual = MockContext.createAndExecute(invokable1,
				Arrays.asList(1, 1, 2, 3, 3));

		assertEquals(expected, actual);
	}

}
