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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.*;

public class EitherTypeInfoTest extends TestLogger {

	Either<Integer, String> intEither = Either.left(1);
	Either<Integer, String> stringEither = Either.right("boo");
	Either<Integer, Tuple2<Double, Long>> tuple2Either = Either.right(new Tuple2<Double, Long>(42.0, 2l));

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testEitherTypeEquality() {
		EitherTypeInfo<Integer, String> eitherInfo1 = new EitherTypeInfo<Integer, String>(
				(Class<Either>) intEither.getClass(), BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		EitherTypeInfo<Integer, String> eitherInfo2 = new EitherTypeInfo<Integer, String>(
				(Class<Either>) stringEither.getClass(), BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		assertEquals(eitherInfo1, eitherInfo2);
		assertEquals(eitherInfo1.hashCode(), eitherInfo2.hashCode());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testEitherTypeInEquality() {
		EitherTypeInfo<Integer, String> eitherInfo1 = new EitherTypeInfo<Integer, String>(
				(Class<Either>) intEither.getClass(), BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		EitherTypeInfo<Integer, Tuple2<Double, Long>> eitherInfo2 = new EitherTypeInfo<Integer, Tuple2<Double, Long>>(
				(Class<Either>) tuple2Either.getClass(), BasicTypeInfo.INT_TYPE_INFO,
				new TupleTypeInfo<Tuple2<Double, Long>>(
						TypeExtractor.getForClass(Double.class), TypeExtractor.getForClass(String.class)));

		assertNotEquals(eitherInfo1, eitherInfo2);
		assertNotEquals(eitherInfo1.hashCode(), eitherInfo2.hashCode());
	}
}
