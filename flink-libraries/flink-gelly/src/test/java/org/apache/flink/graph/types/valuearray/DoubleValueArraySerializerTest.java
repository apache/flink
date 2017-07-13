/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.DoubleValue;

import java.util.Random;

/**
 * A test for the {@link DoubleValueArraySerializer}.
 */
public class DoubleValueArraySerializerTest extends SerializerTestBase<DoubleValueArray> {

	@Override
	protected TypeSerializer<DoubleValueArray> createSerializer() {
		return new DoubleValueArraySerializer();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<DoubleValueArray> getTypeClass() {
		return DoubleValueArray.class;
	}

	@Override
	protected DoubleValueArray[] getTestData() {
		int defaultElements = DoubleValueArray.DEFAULT_CAPACITY_IN_BYTES / DoubleValueArray.ELEMENT_LENGTH_IN_BYTES;

		Random rnd = new Random(874597969123412341L);
		int rndLong = rnd.nextInt();

		DoubleValueArray lva0 = new DoubleValueArray();

		DoubleValueArray lva1 = new DoubleValueArray();
		lva1.addAll(lva0);
		lva1.add(new DoubleValue(0));

		DoubleValueArray lva2 = new DoubleValueArray();
		lva2.addAll(lva1);
		lva2.add(new DoubleValue(1));

		DoubleValueArray lva3 = new DoubleValueArray();
		lva3.addAll(lva2);
		lva3.add(new DoubleValue(-1));

		DoubleValueArray lva4 = new DoubleValueArray();
		lva4.addAll(lva3);
		lva4.add(new DoubleValue(Double.MAX_VALUE));

		DoubleValueArray lva5 = new DoubleValueArray();
		lva5.addAll(lva4);
		lva5.add(new DoubleValue(Double.MIN_VALUE));

		DoubleValueArray lva6 = new DoubleValueArray();
		lva6.addAll(lva5);
		lva6.add(new DoubleValue(rndLong));

		DoubleValueArray lva7 = new DoubleValueArray();
		lva7.addAll(lva6);
		lva7.add(new DoubleValue(-rndLong));

		DoubleValueArray lva8 = new DoubleValueArray();
		lva8.addAll(lva7);
		for (int i = 0; i < 1.5 * defaultElements; i++) {
			lva8.add(new DoubleValue(i));
		}
		lva8.addAll(lva8);

		return new DoubleValueArray[] {lva0, lva1, lva2, lva3, lva4, lva5, lva6, lva7, lva8};
	}
}
