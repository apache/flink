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
package org.apache.flink.graph.test.operations;


import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;

public class CompareResults {
	
	public static <T> void compareResultAsTuples(List<T> result, String expected) {
		compareResult(result, expected, true);
	}

	public static <T> void compareResultAsText(List<T> result, String expected) {
		compareResult(result, expected, false);
	}
	
	private static <T> void compareResult(List<T> result, String expected, boolean asTuples) {
		String[] extectedStrings = expected.split("\n");
		String[] resultStrings = new String[result.size()];
		
		for (int i = 0; i < resultStrings.length; i++) {
			T val = result.get(i);
			
			if (asTuples) {
				if (val instanceof Tuple) {
					Tuple t = (Tuple) val;
					Object first = t.getField(0);
					StringBuilder bld = new StringBuilder(first == null ? "null" : first.toString());
					for (int pos = 1; pos < t.getArity(); pos++) {
						Object next = t.getField(pos);
						bld.append(',').append(next == null ? "null" : next.toString());
					}
					resultStrings[i] = bld.toString();
				}
				else {
					throw new IllegalArgumentException(val + " is no tuple");
				}
			}
			else {
				resultStrings[i] = (val == null) ? "null" : val.toString();
			}
		}
		
		assertEquals("Wrong number of elements result", extectedStrings.length, resultStrings.length);

		Arrays.sort(extectedStrings);
		Arrays.sort(resultStrings);
		
		for (int i = 0; i < extectedStrings.length; i++) {
			assertEquals(extectedStrings[i], resultStrings[i]);
		}
	}

}
