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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.apache.flink.types.Either.Left;
import static org.apache.flink.types.Either.Right;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Either;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Test;

public class EitherSerializerTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testStringDoubleEither() {

	Either<String, Double>[] testData = new Either[] {
			Left("banana"),
			Left(""),
			Right(32.0),
			Right(Double.MIN_VALUE),
			Right(Double.MAX_VALUE)};

	EitherTypeInfo<String, Double> eitherTypeInfo = (EitherTypeInfo<String, Double>) new EitherTypeInfo<String, Double>(
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);
	EitherSerializer<String, Double> eitherSerializer =
			(EitherSerializer<String, Double>) eitherTypeInfo.createSerializer(new ExecutionConfig());
	SerializerTestInstance<Either<String, Double>> testInstance =
			new EitherSerializerTestInstance<Either<String, Double>>(eitherSerializer, eitherTypeInfo.getTypeClass(), -1, testData);
	testInstance.testAll();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEitherWithTuple() {

	Either<Tuple2<Long, Long>, Double>[] testData = new Either[] {
			Either.Left(new Tuple2<>(2l, 9l)),
			new Left<>(new Tuple2<>(Long.MIN_VALUE, Long.MAX_VALUE)),
			new Right<>(32.0),
			Right(Double.MIN_VALUE),
			Right(Double.MAX_VALUE)};

	EitherTypeInfo<Tuple2<Long, Long>, Double> eitherTypeInfo = (EitherTypeInfo<Tuple2<Long, Long>, Double>)
			new EitherTypeInfo<Tuple2<Long, Long>, Double>(
			new TupleTypeInfo<Tuple2<Long, Long>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO),
			BasicTypeInfo.DOUBLE_TYPE_INFO);
	EitherSerializer<Tuple2<Long, Long>, Double> eitherSerializer =
			(EitherSerializer<Tuple2<Long, Long>, Double>) eitherTypeInfo.createSerializer(new ExecutionConfig());
	SerializerTestInstance<Either<Tuple2<Long, Long>, Double>> testInstance =
			new EitherSerializerTestInstance<Either<Tuple2<Long, Long>, Double>>(
					eitherSerializer, eitherTypeInfo.getTypeClass(), -1, testData);
	testInstance.testAll();
	}

	/**
	 * {@link org.apache.flink.api.common.typeutils.SerializerTestBase#testInstantiate()}
	 * checks that the type of the created instance is the same as the type class parameter.
	 * Since we arbitrarily create always create a Left instance we override this test.
	 */
	private class EitherSerializerTestInstance<T> extends SerializerTestInstance<T> {

		public EitherSerializerTestInstance(TypeSerializer<T> serializer,
				Class<T> typeClass, int length, T[] testData) {
			super(serializer, typeClass, length, testData);
		}

		@Override
		@Test
		public void testInstantiate() {
			try {
				TypeSerializer<T> serializer = getSerializer();

				T instance = serializer.createInstance();
				assertNotNull("The created instance must not be null.", instance);
				
				Class<T> type = getTypeClass();
				assertNotNull("The test is corrupt: type class is null.", type);
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				fail("Exception in test: " + e.getMessage());
			}
		}
		
	}
}
