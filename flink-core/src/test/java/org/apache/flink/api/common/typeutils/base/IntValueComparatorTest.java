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

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.IntValue;

import java.util.Random;

public class IntValueComparatorTest extends ComparatorTestBase<IntValue> {

	@Override
	protected TypeComparator<IntValue> createComparator(boolean ascending) {
		return new IntValueComparator(ascending);
	}

	@Override
	protected TypeSerializer<IntValue> createSerializer() {
		return new IntValueSerializer();
	}

	@Override
	protected IntValue[] getSortedTestData() {

		Random rnd = new Random(874597969123412338L);
		int rndInt = rnd.nextInt();
		if (rndInt < 0) {
			rndInt = -rndInt;
		}
		if (rndInt == Integer.MAX_VALUE) {
			rndInt -= 3;
		}
		if (rndInt <= 2) {
			rndInt += 3;
		}
		return new IntValue[]{
			new IntValue(Integer.MIN_VALUE),
			new IntValue(-rndInt),
			new IntValue(-1),
			new IntValue(0),
			new IntValue(1),
			new IntValue(2),
			new IntValue(rndInt),
			new IntValue(Integer.MAX_VALUE)};
	}
}
