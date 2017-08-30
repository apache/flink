/*
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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;

/**
 * Utils for working with the various test harnesses.
 */
public class TestHarnessUtil {

	/**
	 * Extracts the raw elements from the given output list.
	 */
	@SuppressWarnings("unchecked")
	public static <OUT> List<OUT> getRawElementsFromOutput(Queue<Object> output) {
		List<OUT> resultElements = new LinkedList<>();
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
	public static void assertOutputEqualsSorted(String message, Iterable<Object> expected, Iterable<Object> actual, Comparator<Object> comparator) {
		assertEquals(Iterables.size(expected), Iterables.size(actual));

		// first, compare only watermarks, their position should be deterministic
		Iterator<Object> exIt = expected.iterator();
		Iterator<Object> actIt = actual.iterator();
		while (exIt.hasNext()) {
			Object nextEx = exIt.next();
			Object nextAct = actIt.next();
			if (nextEx instanceof Watermark) {
				assertEquals(nextEx, nextAct);
			}
		}

		List<Object> expectedRecords = new ArrayList<>();
		List<Object> actualRecords = new ArrayList<>();

		for (Object ex: expected) {
			if (ex instanceof StreamRecord) {
				expectedRecords.add(ex);
			}
		}

		for (Object act: actual) {
			if (act instanceof StreamRecord) {
				actualRecords.add(act);
			}
		}

		Object[] sortedExpected = expectedRecords.toArray();
		Object[] sortedActual = actualRecords.toArray();

		Arrays.sort(sortedExpected, comparator);
		Arrays.sort(sortedActual, comparator);

		Assert.assertArrayEquals(message, sortedExpected, sortedActual);

	}
}
