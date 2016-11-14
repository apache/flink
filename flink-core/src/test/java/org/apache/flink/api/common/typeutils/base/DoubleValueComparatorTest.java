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
import org.apache.flink.types.DoubleValue;

import java.util.Random;

public class DoubleValueComparatorTest extends ComparatorTestBase<DoubleValue> {

	@Override
	protected TypeComparator<DoubleValue> createComparator(boolean ascending) {
		return new DoubleValueComparator(ascending);
	}

	@Override
	protected TypeSerializer<DoubleValue> createSerializer() {
		return new DoubleValueSerializer();
	}
	
	@Override
	protected DoubleValue[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		double rndDouble = rnd.nextDouble();
		if (rndDouble < 0) {
			rndDouble = -rndDouble;
		}
		if (rndDouble == Double.MAX_VALUE) {
			rndDouble -= 3;
		}
		if (rndDouble <= 2) {
			rndDouble += 3;
		}
		return new DoubleValue[]{
			new DoubleValue(-rndDouble),
			new DoubleValue(-1.0D),
			new DoubleValue(0.0D),
			new DoubleValue(2.0D),
			new DoubleValue(rndDouble),
			new DoubleValue(Double.MAX_VALUE)};
	}
}
