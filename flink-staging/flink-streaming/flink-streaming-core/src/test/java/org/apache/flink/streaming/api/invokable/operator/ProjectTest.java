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
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.StreamProjection;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class ProjectTest implements Serializable {
	private static final long serialVersionUID = 1L;

	@Test
	public void test() {

		TypeInformation<Tuple5<Integer, String, Integer, String, Integer>> inType = TypeExtractor
				.getForObject(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "b",
						4));

		int[] fields = new int[] { 4, 4, 3 };
		Class<?>[] classes = new Class<?>[] { Integer.class, Integer.class, String.class };

		@SuppressWarnings("unchecked")
		ProjectInvokable<Tuple5<Integer, String, Integer, String, Integer>, Tuple3<Integer, Integer, String>> invokable = new ProjectInvokable<Tuple5<Integer, String, Integer, String, Integer>, Tuple3<Integer, Integer, String>>(
				fields,
				(TypeInformation<Tuple3<Integer, Integer, String>>) StreamProjection
						.extractFieldTypes(fields, classes, inType));

		List<Tuple5<Integer, String, Integer, String, Integer>> input = new ArrayList<Tuple5<Integer, String, Integer, String, Integer>>();
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "b", 4));
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "s", 3, "c", 2));
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "c", 2));
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "a", 7));

		List<Tuple3<Integer, Integer, String>> expected = new ArrayList<Tuple3<Integer, Integer, String>>();
		expected.add(new Tuple3<Integer, Integer, String>(4, 4, "b"));
		expected.add(new Tuple3<Integer, Integer, String>(2, 2, "c"));
		expected.add(new Tuple3<Integer, Integer, String>(2, 2, "c"));
		expected.add(new Tuple3<Integer, Integer, String>(7, 7, "a"));

		assertEquals(expected, MockContext.createAndExecute(invokable, input));
	}
}
