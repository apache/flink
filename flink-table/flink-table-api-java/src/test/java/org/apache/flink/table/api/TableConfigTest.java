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

package org.apache.flink.table.api;

import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TableConfig}.
 */
public class TableConfigTest {
	@Test
	public void testReadAndWriteTableConfigOptions() throws Exception{
		Class<?> configClass = TableConfig.class;
		TableConfig config = new TableConfig();
		for (TestSpec<?> spec: testSpecList){
			configClass.getMethod("set" + spec.fieldName, spec.inputClass).invoke(config, spec.inputValue);
			assertEquals(spec.expectedValue, configClass.getMethod("get" + spec.fieldName).invoke(config));
		}
	}

	private static class TestSpec<T> {
		private String fieldName;
		private T expectedValue;
		private T inputValue;
		private Class<?> inputClass;

		static <T> TestSpec<T> start(){
			return new TestSpec<>();
		}

		TestSpec<T> fieldName(String name){
			this.fieldName = name;
			return this;
		}

		TestSpec<T> inputClass(Class<?> inpClass){
			inputClass = inpClass;
			return this;
		}

		TestSpec<T> expect(T value){
			expectedValue = value;
			return this;
		}

		TestSpec<T> input(T value){
			inputValue = value;
			inputClass = value.getClass();
			return this;
		}
	}

	private static final List<TestSpec<?>> testSpecList = Arrays.asList(
		// config used in TableConfig
		TestSpec.<SqlDialect>start().fieldName("SqlDialect").input(SqlDialect.HIVE).expect(SqlDialect.HIVE),
		TestSpec.<Integer>start().fieldName("MaxGeneratedCodeLength").input(50000).expect(50000),
		TestSpec.<Duration>start().fieldName("IdleStateTTL").input(Duration.ofHours(1)).expect(Duration.ofHours(1)),
		TestSpec.<ZoneId>start().fieldName("LocalTimeZone").input(ZoneOffset.ofHours(1)).inputClass(ZoneId.class).expect(ZoneOffset.ofHours(1))
	);
}
