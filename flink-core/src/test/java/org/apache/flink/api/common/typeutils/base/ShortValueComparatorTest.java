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
import org.apache.flink.types.ShortValue;

import java.util.Random;

public class ShortValueComparatorTest extends ComparatorTestBase<ShortValue> {

	@Override
	protected TypeComparator<ShortValue> createComparator(boolean ascending) {
		return new ShortValueComparator(ascending);
	}

	@Override
	protected TypeSerializer<ShortValue> createSerializer() {
		return new ShortValueSerializer();
	}

	@Override
	protected ShortValue[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		short rndShort = Integer.valueOf(rnd.nextInt()).shortValue();
		if (rndShort < 0) {
			rndShort = Integer.valueOf(-rndShort).shortValue();
		}
		if (rndShort == Short.MAX_VALUE) {
			rndShort -= 3;
		}
		if (rndShort <= 2) {
			rndShort += 3;
		}
		return new ShortValue[]{
			new ShortValue(Short.MIN_VALUE),
			new ShortValue(Integer.valueOf(-rndShort).shortValue()),
			new ShortValue(Integer.valueOf(-1).shortValue()),
			new ShortValue(Integer.valueOf(0).shortValue()),
			new ShortValue(Integer.valueOf(1).shortValue()),
			new ShortValue(Integer.valueOf(2).shortValue()),
			new ShortValue(Integer.valueOf(rndShort).shortValue()),
			new ShortValue(Short.MAX_VALUE)};
	}
}
