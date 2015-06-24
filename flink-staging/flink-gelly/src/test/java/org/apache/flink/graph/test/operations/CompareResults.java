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
