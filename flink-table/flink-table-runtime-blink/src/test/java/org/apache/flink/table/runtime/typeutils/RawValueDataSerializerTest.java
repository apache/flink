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
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.utils.RawValueDataAsserter;
import org.apache.flink.testutils.DeeplyEqualsChecker;

/**
 * A test for the {@link RawValueDataSerializer}.
 */
public class RawValueDataSerializerTest extends SerializerTestBase<RawValueData<String>> {
	public RawValueDataSerializerTest() {
		super(new DeeplyEqualsChecker()
			.withCustomCheck(
				(o, o2) -> o instanceof RawValueData && o2 instanceof RawValueData,
				(o, o2, checker) -> RawValueDataAsserter.equivalent(
					(RawValueData) o2,
					new RawValueDataSerializer<>(StringSerializer.INSTANCE)).matches(o)
			));
	}

	@Override
	protected RawValueDataSerializer<String> createSerializer() {
		return new RawValueDataSerializer<>(StringSerializer.INSTANCE);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<RawValueData<String>> getTypeClass() {
		return (Class) RawValueData.class;
	}

	@Override
	protected RawValueData[] getTestData() {
		return new RawValueData[] {
				RawValueData.fromObject("1"),
				RawValueData.fromObject("2"),
				RawValueData.fromObject("3"),
				RawValueData.fromObject("4"),
				RawValueData.fromObject("5")
		};
	}
}
