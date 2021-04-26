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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.types.logical.IntType;

import java.time.DayOfWeek;

/**
 * Test for {@link WrapperTypeInfo}.
 */
public class WrapperTypeInfoTest extends TypeInformationTestBase<WrapperTypeInfo<?>> {

	@Override
	protected WrapperTypeInfo<?>[] getTestData() {
		return new WrapperTypeInfo<?>[] {
				new WrapperTypeInfo<>(
					new IntType(),
					Object.class,
					new KryoSerializer<>(Object.class, new ExecutionConfig())),
				new WrapperTypeInfo<>(
					new IntType(),
					DayOfWeek.class,
					new KryoSerializer<>(DayOfWeek.class, new ExecutionConfig())),
				new WrapperTypeInfo<>(
					new IntType(),
					Integer.class,
					IntSerializer.INSTANCE)
		};
	}
}
