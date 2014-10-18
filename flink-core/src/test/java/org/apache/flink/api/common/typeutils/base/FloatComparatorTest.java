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
import org.apache.flink.api.common.typeutils.base.FloatComparator;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;

public class FloatComparatorTest extends ComparatorTestBase<Float> {

	@Override
	protected TypeComparator<Float> createComparator(boolean ascending) {
		return new FloatComparator(ascending);
	}

	@Override
	protected TypeSerializer<Float> createSerializer() {
		return new FloatSerializer();
	}

	@Override
	protected Float[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		float rndFloat = rnd.nextFloat();
		if (rndFloat < 0) {
			rndFloat = -rndFloat;
		}
		if (rndFloat == Float.MAX_VALUE) {
			rndFloat -= 3;
		}
		if (rndFloat <= 2) {
			rndFloat += 3;
		}
		return new Float[]{
			new Float(-rndFloat),
			new Float(-1.0F),
			new Float(0.0F),
			new Float(2.0F),
			new Float(rndFloat),
			new Float(Float.MAX_VALUE)};
	}
}
