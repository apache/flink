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

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.List;

/**
 * List of matchers that might be useful in the table ecosystem.
 */
@Internal
public final class TableTestMatchers {

	public static Matcher<List<Row>> deepEqualTo(List<Row> rows, boolean ignoreOrder) {
		return new BaseMatcher<List<Row>>() {

			@Override
			public void describeTo(Description description) {
				description.appendValueList("", "\n", "", rows);
			}

			@Override
			@SuppressWarnings("unchecked")
			public void describeMismatch(Object item, Description description) {
				description.appendText("was ").appendValueList("", "\n", "", (List<Row>) item);
			}

			@Override
			@SuppressWarnings("unchecked")
			public boolean matches(Object item) {
				return RowUtils.compareRows(rows, (List<Row>) item, ignoreOrder);
			}
		};
	}
}
