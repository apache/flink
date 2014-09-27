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

package org.apache.flink.api.common.typeutils.base;

import java.util.Random;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteComparator;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;

public class ByteComparatorTest extends ComparatorTestBase<Byte> {

	@Override
	protected TypeComparator<Byte> createComparator(boolean ascending) {
		return new ByteComparator(ascending);
	}

	@Override
	protected TypeSerializer<Byte> createSerializer() {
		return new ByteSerializer();
	}

	@Override
	protected Byte[] getSortedTestData() {

		Random rnd = new Random(874597969123412338L);
		int rndByte = rnd.nextInt(Byte.MAX_VALUE);
		if (rndByte < 0) {
			rndByte = -rndByte;
		}
		if (rndByte == Byte.MAX_VALUE) {
			rndByte -= 3;
		}
		if (rndByte <= 2) {
			rndByte += 3;
		}
		return new Byte[]{
			new Byte(Byte.MIN_VALUE),
			new Byte(new Integer(-rndByte).byteValue()),
			new Byte(new Integer(-1).byteValue()),
			new Byte(new Integer(0).byteValue()),
			new Byte(new Integer(1).byteValue()),
			new Byte(new Integer(2).byteValue()),
			new Byte(new Integer(rndByte).byteValue()),
			new Byte(Byte.MAX_VALUE)};
	}
}
