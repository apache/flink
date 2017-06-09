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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Tests for the {@link WritableComparator}.
 */
public class WritableComparatorTest extends ComparatorTestBase<StringArrayWritable> {

	StringArrayWritable[] data = new StringArrayWritable[]{
			new StringArrayWritable(new String[]{}),
			new StringArrayWritable(new String[]{""}),
			new StringArrayWritable(new String[]{"a", "a"}),
			new StringArrayWritable(new String[]{"a", "b"}),
			new StringArrayWritable(new String[]{"c", "c"}),
			new StringArrayWritable(new String[]{"d", "f"}),
			new StringArrayWritable(new String[]{"d", "m"}),
			new StringArrayWritable(new String[]{"z", "x"}),
			new StringArrayWritable(new String[]{"a", "a", "a"})
	};

	@Override
	protected TypeComparator<StringArrayWritable> createComparator(boolean ascending) {
		return new WritableComparator<StringArrayWritable>(ascending, StringArrayWritable.class);
	}

	@Override
	protected TypeSerializer<StringArrayWritable> createSerializer() {
		return new WritableSerializer<StringArrayWritable>(StringArrayWritable.class);
	}

	@Override
	protected StringArrayWritable[] getSortedTestData() {
		return data;
	}
}
