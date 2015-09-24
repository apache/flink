/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.util;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Utils for working with the various test harnesses.
 */
public class TestHarnessUtil {
	/**
	 * Extracts the StreamRecords from the given output list.
	 */
	@SuppressWarnings("unchecked")
	public static <OUT> List<StreamRecord<OUT>> getStreamRecordsFromOutput(List<Object> output) {
		List<StreamRecord<OUT>> resultElements = new LinkedList<StreamRecord<OUT>>();
		for (Object e: output) {
			if (e instanceof StreamRecord) {
				resultElements.add((StreamRecord<OUT>) e);
			}
		}
		return resultElements;
	}

	/**
	 * Extracts the raw elements from the given output list.
	 */
	@SuppressWarnings("unchecked")
	public static <OUT> List<OUT> getRawElementsFromOutput(Queue<Object> output) {
		List<OUT> resultElements = new LinkedList<OUT>();
		for (Object e: output) {
			if (e instanceof StreamRecord) {
				resultElements.add(((StreamRecord<OUT>) e).getValue());
			}
		}
		return resultElements;
	}

	/**
	 * Compare the two queues containing operator/task output by converting them to an array first.
	 */
	public static void assertOutputEquals(String message, Queue<Object> expected, Queue<Object> actual) {
		Assert.assertArrayEquals(message,
				expected.toArray(),
				actual.toArray());

	}

	/**
	 * Compare the two queues containing operator/task output by converting them to an array first.
	 */
	public static void assertOutputEqualsSorted(String message, Queue<Object> expected, Queue<Object> actual, Comparator<Object> comparator) {
		Object[] sortedExpected = expected.toArray();
		Object[] sortedActual = actual.toArray();

		Arrays.sort(sortedExpected, comparator);
		Arrays.sort(sortedActual, comparator);

		Assert.assertArrayEquals(message, sortedExpected, sortedActual);

	}
}
