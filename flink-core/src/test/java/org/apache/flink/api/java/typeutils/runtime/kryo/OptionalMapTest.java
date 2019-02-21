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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.java.typeutils.runtime.kryo.OptionalMap.MergeResult;

import org.junit.Test;

import java.util.LinkedHashMap;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link OptionalMap}.
 */
public class OptionalMapTest {

	@Test
	public void usageExample() {
		OptionalMap<Class<?>, String> map = new OptionalMap<>();

		map.put("java.lang.String", String.class, "a string class");
		map.put("scala.Option", null, "a scala Option");
		map.put("java.lang.Boolean", Boolean.class, null);

		assertThat(map.keyNames(), hasItems("java.lang.String", "scala.Option"));
		assertThat(map.absentKeysOrValues(), hasItems("scala.Option", "java.lang.Boolean"));
	}

	@Test
	public void overridingKeyWithTheSameKeyName() {
		OptionalMap<Class<?>, String> map = new OptionalMap<>();

		map.put("java.lang.String", null, "a string class");
		map.put("java.lang.String", String.class, "a string class");

		assertThat(map.absentKeysOrValues(), is(empty()));
	}

	@Test
	public void overridingKeysAndValuesWithTheSameKeyName() {
		OptionalMap<Class<?>, String> map = new OptionalMap<>();

		map.put("java.lang.String", null, null);
		map.put("java.lang.String", String.class, "a string class");

		assertThat(map.absentKeysOrValues(), is(empty()));
	}

	@Test
	public void overridingAValueWithMissingKeyShouldBeConsideredAsAbsent() {
		OptionalMap<Class<?>, String> map = new OptionalMap<>();

		map.put("java.lang.String", null, null);
		map.put("java.lang.String", null, "a string class");

		assertThat(map.absentKeysOrValues(), hasItem("java.lang.String"));
	}

	@Test
	public void mergingMapsWithPresentEntriesLeavesNoAbsentKeyNames() {
		OptionalMap<Class<?>, String> first = new OptionalMap<>();
		first.put("b", null, null);
		first.put("c", String.class, null);

		OptionalMap<Class<?>, String> second = new OptionalMap<>();
		second.put("a", String.class, "aaa");
		second.put("b", String.class, "bbb");
		second.put("c", Void.class, "ccc");
		second.put("d", String.class, "ddd");

		first.putAll(second);

		assertThat(first.absentKeysOrValues(), is(empty()));
	}

	@Test
	public void mergingMapsPreserversTheOrderOfTheOriginalMap() {
		OptionalMap<Class<?>, String> first = new OptionalMap<>();
		first.put("b", null, null);
		first.put("c", String.class, null);

		OptionalMap<Class<?>, String> second = new OptionalMap<>();
		second.put("a", String.class, "aaa");
		second.put("b", String.class, "bbb");
		second.put("c", Void.class, "ccc");
		second.put("d", String.class, "ddd");

		first.putAll(second);

		assertThat(first.keyNames(), contains("b", "c", "a", "d"));
	}

	@Test
	public void mergingToEmpty() {
		OptionalMap<Class<?>, String> first = new OptionalMap<>();

		OptionalMap<Class<?>, String> second = new OptionalMap<>();
		second.put("a", String.class, "aaa");
		second.put("b", String.class, "bbb");
		second.put("c", Void.class, "ccc");
		second.put("d", String.class, "ddd");

		first.putAll(second);

		assertThat(first.keyNames(), contains("a", "b", "c", "d"));
	}

	@Test(expected = IllegalStateException.class)
	public void unwrapOptionalsWithMissingValueThrows() {
		OptionalMap<Class<?>, String> map = new OptionalMap<>();

		map.put("a", String.class, null);

		map.unwrapOptionals();
	}

	@Test(expected = IllegalStateException.class)
	public void unwrapOptionalsWithMissingKeyThrows() {
		OptionalMap<Class<?>, String> map = new OptionalMap<>();

		map.put("a", null, "blabla");

		map.unwrapOptionals();
	}

	@Test
	public void unwrapOptionalsPreservesOrder() {
		OptionalMap<Class<?>, String> map = new OptionalMap<>();

		map.put("a", String.class, "aaa");
		map.put("b", Boolean.class, "bbb");

		LinkedHashMap<Class<?>, String> m = map.unwrapOptionals();

		assertThat(m.keySet(), contains(String.class, Boolean.class));
		assertThat(m.values(), contains("aaa", "bbb"));
	}

	@Test
	public void testPrefix() {
		OptionalMap<Class<?>, String> left = new OptionalMap<>();

		left.put("a", String.class, "aaa");
		left.put("b", String.class, "aaa");

		OptionalMap<Class<?>, String> right = new OptionalMap<>(left);

		right.put("c", Boolean.class, "bbb");

		assertTrue(OptionalMap.isLeftPrefixOfRight(left, right));
	}

	@Test
	public void testNonPrefix() {
		OptionalMap<Class<?>, String> left = new OptionalMap<>();

		left.put("a", String.class, "aaa");
		left.put("c", String.class, "aaa");

		OptionalMap<Class<?>, String> right = new OptionalMap<>();

		right.put("b", Boolean.class, "bbb");
		right.put("c", Boolean.class, "bbb");


		assertFalse(OptionalMap.isLeftPrefixOfRight(left, right));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void demoMergeResult() {
		OptionalMap<Class<?>, String> left = new OptionalMap<>();
		left.put("b", null, null);
		left.put("c", String.class, null);

		OptionalMap<Class<?>, String> right = new OptionalMap<>();
		right.put("b", String.class, "bbb");
		right.put("c", Void.class, "ccc");
		right.put("a", Boolean.class, "aaa");
		right.put("d", Long.class, "ddd");

		MergeResult<Class<?>, String> result = OptionalMap.mergeRightIntoLeft(left, right);

		assertThat(result.hasMissingKeys(), is(false));
		assertThat(result.isOrderedSubset(), is(true));
		assertThat(result.missingKeys(), is(empty()));

		LinkedHashMap<Class<?>, String> merged = result.getMerged();
		assertThat(merged.keySet(), contains(String.class, Void.class, Boolean.class, Long.class));
	}
}
