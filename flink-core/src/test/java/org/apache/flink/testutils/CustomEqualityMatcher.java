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

package org.apache.flink.testutils;

import org.apache.flink.api.java.tuple.Tuple;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.Arrays;

public class CustomEqualityMatcher extends BaseMatcher<Object> {

	private final Object wanted;

	private final DeeplyEqualsChecker checker;

	private CustomEqualityMatcher(Object wanted, DeeplyEqualsChecker checker) {
		this.wanted = wanted;
		this.checker = checker;
	}

	/**
	 * This matcher performs similar comparison to {@link org.hamcrest.core.IsEqual}, which resembles
	 * {@link java.util.Objects#deepEquals(Object, Object)} logic. The only difference here is that {@link Tuple}s
	 * are treated similarly to arrays.
	 *
	 * <p>This means that if we compare two Tuples that contain arrays, those arrays will
	 * be compared with {@link Arrays#deepEquals(Object[], Object[])} rather than with reference comparison.
	 *
	 * @param item expected value
	 */
	public static CustomEqualityMatcher deeplyEquals(Object item) {
		return new CustomEqualityMatcher(item, new DeeplyEqualsChecker());
	}

	/**
	 * Performs assertions with this customly configured {@link DeeplyEqualsChecker}. It might have some additional
	 * rules applied.
	 */
	public CustomEqualityMatcher withChecker(DeeplyEqualsChecker checker) {
		return new CustomEqualityMatcher(wanted, checker);
	}

	@Override
	public boolean matches(Object item) {
		return checker.deepEquals(item, wanted);
	}

	@Override
	public void describeTo(Description description) {
		description.appendValue(wanted);
	}
}
