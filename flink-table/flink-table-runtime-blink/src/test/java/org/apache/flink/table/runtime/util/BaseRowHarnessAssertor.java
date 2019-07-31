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

package org.apache.flink.table.runtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Utils for working with the various window test harnesses.
 */
public class BaseRowHarnessAssertor {

	private final TypeInformation[] typeInfos;
	private final Comparator<GenericRow> comparator;

	public BaseRowHarnessAssertor(TypeInformation[] typeInfos, Comparator<GenericRow> comparator) {
		this.typeInfos = typeInfos;
		this.comparator = comparator;
	}

	public BaseRowHarnessAssertor(TypeInformation[] typeInfos) {
		this(typeInfos, new StringComparator());
	}


	/**
	 * Compare the two queues containing operator/task output by converting them to an array first.
	 * Asserts two converted array should be same.
	 */
	public void assertOutputEquals(
			String message,
			Collection<Object> expected,
			Collection<Object> actual) {
		assertOutputEquals(message, expected, actual, false);
	}

	/**
	 * Compare the two queues containing operator/task output by converting them to an array first, sort array by
	 * comparator. Assertes two sorted converted array should be same.
	 */
	public void assertOutputEqualsSorted(
		String message,
		Collection<Object> expected,
		Collection<Object> actual) {
		assertOutputEquals(message, expected, actual, true);
	}

	private void assertOutputEquals(
			String message,
			Collection<Object> expected,
			Collection<Object> actual,
			boolean needSort) {
		if (needSort) {
			Preconditions.checkArgument(comparator != null, "Comparator should not be null!");
		}
		assertEquals(expected.size(), actual.size());

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

		List<GenericRow> expectedRecords = new ArrayList<>();
		List<GenericRow> actualRecords = new ArrayList<>();

		for (Object ex : expected) {
			if (ex instanceof StreamRecord) {
				expectedRecords.add((GenericRow) ((StreamRecord) ex).getValue());
			}
		}

		for (Object act : actual) {
			if (act instanceof StreamRecord) {
				BaseRow actualOutput = (BaseRow) ((StreamRecord) act).getValue();
				// joined row can't equals to generic row, so cast joined row to generic row first
				GenericRow actualRow = BaseRowUtil.toGenericRow(
						actualOutput,
						Arrays.stream(typeInfos)
								.map(TypeInfoLogicalTypeConverter::fromTypeInfoToLogicalType)
								.toArray(LogicalType[]::new));
				actualRecords.add(actualRow);
			}
		}

		GenericRow[] sortedExpected = expectedRecords.toArray(new GenericRow[expectedRecords.size()]);
		GenericRow[] sortedActual = actualRecords.toArray(new GenericRow[actualRecords.size()]);

		if (needSort) {
			Arrays.sort(sortedExpected, comparator);
			Arrays.sort(sortedActual, comparator);
		}

		Assert.assertArrayEquals(message, sortedExpected, sortedActual);
	}

	private static class StringComparator implements Comparator<GenericRow> {
		@Override
		public int compare(GenericRow o1, GenericRow o2) {
			return o1.toString().compareTo(o2.toString());
		}
	}
}
