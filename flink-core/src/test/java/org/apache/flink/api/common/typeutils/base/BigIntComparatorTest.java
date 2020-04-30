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

import java.math.BigInteger;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class BigIntComparatorTest extends ComparatorTestBase<BigInteger> {

	@Override
	protected TypeComparator<BigInteger> createComparator(boolean ascending) {
		return new BigIntComparator(ascending);
	}

	@Override
	protected TypeSerializer<BigInteger> createSerializer() {
		return new BigIntSerializer();
	}

	@Override
	protected BigInteger[] getSortedTestData() {
		return new BigInteger[] {
			new BigInteger("-8745979691234123413478523984729447"),
			BigInteger.valueOf(-10000),
			BigInteger.valueOf(-1),
			BigInteger.ZERO,
			BigInteger.ONE,
			BigInteger.TEN,
			new BigInteger("127"),
			new BigInteger("128"),
			new BigInteger("129"),
			new BigInteger("130"),
			BigInteger.valueOf(0b10000000_00000000_00000000_00000000L),
			BigInteger.valueOf(0b10000000_00000000_00000000_00000001L),
			BigInteger.valueOf(0b10000000_00000000_10000000_00000000L),
			new BigInteger("8745979691234123413478523984729447")
		};
	}
}
