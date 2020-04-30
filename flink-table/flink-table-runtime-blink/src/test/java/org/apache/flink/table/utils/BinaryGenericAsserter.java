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

package org.apache.flink.table.utils;

import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.runtime.typeutils.BinaryGenericSerializer;
import org.apache.flink.table.runtime.util.SegmentsUtil;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;

/**
 * A {@link org.hamcrest.Matcher} that allows equality check on {@link BinaryGeneric}s.
 */
public class BinaryGenericAsserter extends TypeSafeMatcher<BinaryGeneric> {
	private final BinaryGeneric expected;
	private final BinaryGenericSerializer serializer;

	private BinaryGenericAsserter(
			BinaryGeneric expected,
			BinaryGenericSerializer serializer) {
		this.expected = expected;
		this.serializer = serializer;
	}

	/**
	 * Checks that the {@link BinaryGeneric} is equivalent to the expected one. The serializer will be used
	 * to ensure both objects are materialized into the binary form.
	 *
	 * @param expected the expected object
	 * @param serializer serializer used to materialize the underlying java object
	 * @return binary equality matcher
	 */
	@SuppressWarnings("unchecked")
	public static BinaryGenericAsserter equivalent(BinaryGeneric expected, BinaryGenericSerializer serializer) {
		expected.ensureMaterialized(serializer.getInnerSerializer());
		return new BinaryGenericAsserter(expected, serializer);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected boolean matchesSafely(BinaryGeneric item) {
		item.ensureMaterialized(serializer.getInnerSerializer());
		expected.ensureMaterialized(serializer.getInnerSerializer());

		return item.getSizeInBytes() == expected.getSizeInBytes() &&
			SegmentsUtil.equals(
				item.getSegments(),
				item.getOffset(),
				expected.getSegments(),
				expected.getOffset(),
				item.getSizeInBytes());
	}

	@Override
	public void describeTo(Description description) {
		byte[] bytes = SegmentsUtil.getBytes(expected.getSegments(), expected.getOffset(), expected.getSizeInBytes());
		description.appendText(Arrays.toString(bytes));
	}
}
