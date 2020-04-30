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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.LongValue;

import java.util.Random;

/**
 * A test for the {@link LongValueArraySerializer}.
 */
public class LongValueArraySerializerTest extends ValueArraySerializerTestBase<LongValueArray> {

	@Override
	protected TypeSerializer<LongValueArray> createSerializer() {
		return new LongValueArraySerializer();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<LongValueArray> getTypeClass() {
		return LongValueArray.class;
	}

	@Override
	protected LongValueArray[] getTestData() {
		int defaultElements = LongValueArray.DEFAULT_CAPACITY_IN_BYTES / LongValueArray.ELEMENT_LENGTH_IN_BYTES;

		Random rnd = new Random(874597969123412341L);
		long rndLong = rnd.nextLong();

		LongValueArray lva0 = new LongValueArray();

		LongValueArray lva1 = new LongValueArray();
		lva1.addAll(lva0);
		lva1.add(new LongValue(0));

		LongValueArray lva2 = new LongValueArray();
		lva2.addAll(lva1);
		lva2.add(new LongValue(1));

		LongValueArray lva3 = new LongValueArray();
		lva3.addAll(lva2);
		lva3.add(new LongValue(-1));

		LongValueArray lva4 = new LongValueArray();
		lva4.addAll(lva3);
		lva4.add(new LongValue(Long.MAX_VALUE));

		LongValueArray lva5 = new LongValueArray();
		lva5.addAll(lva4);
		lva5.add(new LongValue(Long.MIN_VALUE));

		LongValueArray lva6 = new LongValueArray();
		lva6.addAll(lva5);
		lva6.add(new LongValue(rndLong));

		LongValueArray lva7 = new LongValueArray();
		lva7.addAll(lva6);
		lva7.add(new LongValue(-rndLong));

		LongValueArray lva8 = new LongValueArray();
		lva8.addAll(lva7);
		for (int i = 0; i < 1.5 * defaultElements; i++) {
			lva8.add(new LongValue(i));
		}
		lva8.addAll(lva8);

		return new LongValueArray[] {lva0, lva1, lva2, lva3, lva4, lva5, lva6, lva7, lva8};
	}
}
