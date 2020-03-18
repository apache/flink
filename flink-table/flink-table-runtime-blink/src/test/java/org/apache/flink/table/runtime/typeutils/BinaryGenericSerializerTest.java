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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.utils.BinaryGenericAsserter;
import org.apache.flink.testutils.DeeplyEqualsChecker;

/**
 * A test for the {@link BinaryGenericSerializer}.
 */
public class BinaryGenericSerializerTest extends SerializerTestBase<BinaryGeneric<String>> {
	public BinaryGenericSerializerTest() {
		super(new DeeplyEqualsChecker()
			.withCustomCheck(
				(o, o2) -> o instanceof BinaryGeneric && o2 instanceof BinaryGeneric,
				(o, o2, checker) -> BinaryGenericAsserter.equivalent(
					(BinaryGeneric) o2,
					new BinaryGenericSerializer<>(StringSerializer.INSTANCE)).matches(o)
			));
	}

	@Override
	protected BinaryGenericSerializer<String> createSerializer() {
		return new BinaryGenericSerializer<>(StringSerializer.INSTANCE);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<BinaryGeneric<String>> getTypeClass() {
		return (Class) BinaryGeneric.class;
	}

	@Override
	protected BinaryGeneric[] getTestData() {
		return new BinaryGeneric[] {
				new BinaryGeneric<>("1"),
				new BinaryGeneric<>("2"),
				new BinaryGeneric<>("3"),
				new BinaryGeneric<>("4"),
				new BinaryGeneric<>("5")
		};
	}
}
