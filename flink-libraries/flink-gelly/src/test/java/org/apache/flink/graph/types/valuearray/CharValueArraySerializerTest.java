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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.CharValue;

import java.util.Random;

/**
 * A test for the {@link CharValueArraySerializer}.
 */
public class CharValueArraySerializerTest extends ValueArraySerializerTestBase<CharValueArray> {

	@Override
	protected TypeSerializer<CharValueArray> createSerializer() {
		return new CharValueArraySerializer();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<CharValueArray> getTypeClass() {
		return CharValueArray.class;
	}

	@Override
	protected CharValueArray[] getTestData() {
		int defaultElements = CharValueArray.DEFAULT_CAPACITY_IN_BYTES / CharValueArray.ELEMENT_LENGTH_IN_BYTES;

		Random rnd = new Random(874597969123412341L);
		int rndLong = rnd.nextInt();

		CharValueArray lva0 = new CharValueArray();

		CharValueArray lva1 = new CharValueArray();
		lva1.addAll(lva0);
		lva1.add(new CharValue((char) 0));

		CharValueArray lva2 = new CharValueArray();
		lva2.addAll(lva1);
		lva2.add(new CharValue((char) 1));

		CharValueArray lva3 = new CharValueArray();
		lva3.addAll(lva2);
		lva3.add(new CharValue((char) -1));

		CharValueArray lva4 = new CharValueArray();
		lva4.addAll(lva3);
		lva4.add(new CharValue(Character.MAX_VALUE));

		CharValueArray lva5 = new CharValueArray();
		lva5.addAll(lva4);
		lva5.add(new CharValue(Character.MIN_VALUE));

		CharValueArray lva6 = new CharValueArray();
		lva6.addAll(lva5);
		lva6.add(new CharValue((char) rndLong));

		CharValueArray lva7 = new CharValueArray();
		lva7.addAll(lva6);
		lva7.add(new CharValue((char) -rndLong));

		CharValueArray lva8 = new CharValueArray();
		lva8.addAll(lva7);
		for (int i = 0; i < 1.5 * defaultElements; i++) {
			lva8.add(new CharValue((char) i));
		}
		lva8.addAll(lva8);

		return new CharValueArray[] {lva0, lva1, lva2, lva3, lva4, lva5, lva6, lva7, lva8};
	}
}
