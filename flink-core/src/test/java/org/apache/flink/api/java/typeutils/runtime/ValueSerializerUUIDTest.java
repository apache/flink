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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.UUID;

public class ValueSerializerUUIDTest extends SerializerTestBase<ValueID> {
	@Override
	protected TypeSerializer<ValueID> createSerializer() {
		return new ValueSerializer<>(ValueID.class);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<ValueID> getTypeClass() {
		return ValueID.class;
	}

	@Override
	protected ValueID[] getTestData() {
		return new ValueID[] {
			new ValueID(new UUID(0, 0)),
			new ValueID(new UUID(1, 0)),
			new ValueID(new UUID(1, 1))
		};
	}
}
