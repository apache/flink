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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.StringValue;

import java.util.Random;

/**
 * A test for the {@link StringValueArraySerializer}.
 */
public class StringValueArraySerializerTest extends SerializerTestBase<StringValueArray> {

	@Override
	protected TypeSerializer<StringValueArray> createSerializer() {
		return new StringValueArraySerializer();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<StringValueArray> getTypeClass() {
		return StringValueArray.class;
	}

	@Override
	protected StringValueArray[] getTestData() {
		int defaultElements = StringValueArray.DEFAULT_CAPACITY_IN_BYTES;

		Random rnd = new Random(874597969123412341L);
		long rndLong = rnd.nextLong();

		StringValueArray sva0 = new StringValueArray();

		StringValueArray sva1 = new StringValueArray();
		sva1.addAll(sva0);
		sva1.add(new StringValue(String.valueOf(0)));

		StringValueArray sva2 = new StringValueArray();
		sva2.addAll(sva1);
		sva2.add(new StringValue(String.valueOf(1)));

		StringValueArray sva3 = new StringValueArray();
		sva3.addAll(sva2);
		sva3.add(new StringValue(String.valueOf(-1)));

		StringValueArray sva4 = new StringValueArray();
		sva4.addAll(sva3);
		sva4.add(new StringValue(String.valueOf(Long.MAX_VALUE)));

		StringValueArray sva5 = new StringValueArray();
		sva5.addAll(sva4);
		sva5.add(new StringValue(String.valueOf(Long.MIN_VALUE)));

		StringValueArray sva6 = new StringValueArray();
		sva6.addAll(sva5);
		sva6.add(new StringValue(String.valueOf(rndLong)));

		StringValueArray sva7 = new StringValueArray();
		sva7.addAll(sva6);
		sva7.add(new StringValue(String.valueOf(-rndLong)));

		StringValueArray sva8 = new StringValueArray();
		sva8.addAll(sva7);
		for (int i = 0; i < 1.5 * defaultElements; i++) {
			sva8.add(new StringValue(String.valueOf(i)));
		}
		sva8.addAll(sva8);

		return new StringValueArray[] {sva0, sva1, sva2, sva3, sva4, sva5, sva6, sva7, sva8};
	}
}
