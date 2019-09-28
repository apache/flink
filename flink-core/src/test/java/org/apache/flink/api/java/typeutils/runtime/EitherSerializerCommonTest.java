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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.types.Either;

public class EitherSerializerCommonTest extends SerializerTestBase<Either<String, Integer>> {

	@Override
	protected TypeSerializer<Either<String, Integer>> createSerializer() {
		return new EitherSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<Either<String, Integer>> getTypeClass() {
		return (Class<Either<String, Integer>>) (Class<?>) Either.class;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Either<String, Integer>[] getTestData() {
		return new Either[] {
				new Either.Left("hello friend"),
				new Either.Left("hello friend"),
				new Either.Right(37),
				new Either.Left("hello friend"),
				new Either.Right(1569653),
				new Either.Left("hello friend")
		};
	}
}
