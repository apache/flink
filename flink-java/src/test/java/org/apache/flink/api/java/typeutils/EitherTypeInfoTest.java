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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.Either.Right;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

public class EitherTypeInfoTest extends TestLogger {

	Either<Integer, String> intEither = Either.Left(1);
	Either<Integer, String> stringEither = Either.Right("boo");
	Either<Integer, Tuple2<Double, Long>> tuple2Either = new Right<>(new Tuple2<Double, Long>(42.0, 2l));

	@Test
	public void testEitherTypeEquality() {
		EitherTypeInfo<Integer, String> eitherInfo1 = new EitherTypeInfo<Integer, String>(
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		EitherTypeInfo<Integer, String> eitherInfo2 = new EitherTypeInfo<Integer, String>(
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		assertEquals(eitherInfo1, eitherInfo2);
		assertEquals(eitherInfo1.hashCode(), eitherInfo2.hashCode());
	}

	@Test
	public void testEitherTypeInEquality() {
		EitherTypeInfo<Integer, String> eitherInfo1 = new EitherTypeInfo<Integer, String>(
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		EitherTypeInfo<Integer, Tuple2<Double, Long>> eitherInfo2 = new EitherTypeInfo<Integer, Tuple2<Double, Long>>(
				BasicTypeInfo.INT_TYPE_INFO, new TupleTypeInfo<Tuple2<Double, Long>>(
				TypeExtractor.getForClass(Double.class), TypeExtractor.getForClass(String.class)));

		assertNotEquals(eitherInfo1, eitherInfo2);
		assertNotEquals(eitherInfo1.hashCode(), eitherInfo2.hashCode());
	}
}
