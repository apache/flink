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
import org.apache.flink.api.common.typeutils.base.ShortComparator;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;

public class ShortComparatorTest extends ComparatorTestBase<Short> {

	@Override
	protected TypeComparator<Short> createComparator(boolean ascending) {
		return new ShortComparator(ascending);
	}

	@Override
	protected TypeSerializer<Short> createSerializer() {
		return new ShortSerializer();
	}

	@Override
	protected Short[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		short rndShort = new Integer(rnd.nextInt()).shortValue();
		if (rndShort < 0) {
			rndShort = new Integer(-rndShort).shortValue();
		}
		if (rndShort == Short.MAX_VALUE) {
			rndShort -= 3;
		}
		if (rndShort <= 2) {
			rndShort += 3;
		}
		return new Short[]{
			new Short(Short.MIN_VALUE),
			new Short(new Integer(-rndShort).shortValue()),
			new Short(new Integer(-1).shortValue()),
			new Short(new Integer(0).shortValue()),
			new Short(new Integer(1).shortValue()),
			new Short(new Integer(2).shortValue()),
			new Short(new Integer(rndShort).shortValue()),
			new Short(Short.MAX_VALUE)};
	}
}
