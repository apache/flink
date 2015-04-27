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

package org.apache.flink.streaming.api.operators;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.operators.StreamGroupedFold;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class GroupedFoldTest {

	private static class MyFolder implements FoldFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String accumulator, Integer value) throws Exception {
			return accumulator + value.toString();
		}

	}

	@Test
	public void test() {
		TypeInformation<String> outType = TypeExtractor.getForObject("A string");

		StreamGroupedFold<Integer, String> operator1 = new StreamGroupedFold<Integer, String>(
				new MyFolder(), new KeySelector<Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Integer value) throws Exception {
				return value.toString();
			}
		}, "100", outType);

		List<String> expected = Arrays.asList("1001","10011", "1002", "10022", "1003");
		List<String> actual = MockContext.createAndExecute(operator1,
				Arrays.asList(1, 1, 2, 2, 3));

		assertEquals(expected, actual);
	}
}
