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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.DataTypes;

import java.nio.ByteBuffer;
import java.time.DayOfWeek;

/**
 * Test for {@link ExternalTypeInfo}.
 */
public class ExternalTypeInfoTest extends TypeInformationTestBase<ExternalTypeInfo<?>> {

	@Override
	protected ExternalTypeInfo<?>[] getTestData() {
		return new ExternalTypeInfo<?>[] {
				ExternalTypeInfo.of(
					DataTypes.RAW(DayOfWeek.class, new KryoSerializer<>(DayOfWeek.class, new ExecutionConfig()))),
				ExternalTypeInfo.of(
					DataTypes.RAW(ByteBuffer.class, new KryoSerializer<>(ByteBuffer.class, new ExecutionConfig()))),
		};
	}
}
