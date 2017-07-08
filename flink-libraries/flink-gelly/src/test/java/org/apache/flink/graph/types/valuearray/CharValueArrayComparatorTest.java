/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.CharValue;

/**
 * Tests for {@link CharValueArrayComparator}.
 */
public class CharValueArrayComparatorTest extends ComparatorTestBase<CharValueArray> {

	@Override
	protected TypeComparator<CharValueArray> createComparator(boolean ascending) {
		return new CharValueArrayComparator(ascending);
	}

	@Override
	protected TypeSerializer<CharValueArray> createSerializer() {
		return new CharValueArraySerializer();
	}

	@Override
	protected CharValueArray[] getSortedTestData() {
		CharValueArray lva0 = new CharValueArray();

		CharValueArray lva1 = new CharValueArray();
		lva1.add(new CharValue((char) 5));

		CharValueArray lva2 = new CharValueArray();
		lva2.add(new CharValue((char) 5));
		lva2.add(new CharValue((char) 10));

		return new CharValueArray[]{ lva0, lva1 };
	}
}
