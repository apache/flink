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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

public class StringComparatorTest extends ComparatorTestBase<String> {

	@Override
	protected TypeComparator<String> createComparator(boolean ascending) {
		return new StringComparator(ascending);
	}

	@Override
	protected TypeSerializer<String> createSerializer() {
		return new StringSerializer();
	}

	@Override
	protected String[] getSortedTestData() {
		return new String[]{
			"",
			"Lorem Ipsum Dolor Omit Longer",
			"aaaa",
			"abcd",
			"abce",
			"abdd",
			"accd",
			"bbcd"
		};
	}
}
