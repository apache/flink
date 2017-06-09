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
import org.apache.flink.types.IntValue;

import java.util.Random;

/**
 * A test for the {@link IntValueArraySerializer}.
 */
public class IntValueArraySerializerTest extends SerializerTestBase<IntValueArray> {

	@Override
	protected TypeSerializer<IntValueArray> createSerializer() {
		return new IntValueArraySerializer();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<IntValueArray> getTypeClass() {
		return IntValueArray.class;
	}

	@Override
	protected IntValueArray[] getTestData() {
		int defaultElements = IntValueArray.DEFAULT_CAPACITY_IN_BYTES / IntValueArray.ELEMENT_LENGTH_IN_BYTES;

		Random rnd = new Random(874597969123412341L);
		int rndInt = rnd.nextInt();

		IntValueArray iva0 = new IntValueArray();

		IntValueArray iva1 = new IntValueArray();
		iva1.addAll(iva0);
		iva1.add(new IntValue(0));

		IntValueArray iva2 = new IntValueArray();
		iva2.addAll(iva1);
		iva2.add(new IntValue(1));

		IntValueArray iva3 = new IntValueArray();
		iva3.addAll(iva2);
		iva3.add(new IntValue(-1));

		IntValueArray iva4 = new IntValueArray();
		iva4.addAll(iva3);
		iva4.add(new IntValue(Integer.MAX_VALUE));

		IntValueArray iva5 = new IntValueArray();
		iva5.addAll(iva4);
		iva5.add(new IntValue(Integer.MIN_VALUE));

		IntValueArray iva6 = new IntValueArray();
		iva6.addAll(iva5);
		iva6.add(new IntValue(rndInt));

		IntValueArray iva7 = new IntValueArray();
		iva7.addAll(iva6);
		iva7.add(new IntValue(-rndInt));

		IntValueArray iva8 = new IntValueArray();
		iva8.addAll(iva7);
		for (int i = 0; i < 1.5 * defaultElements; i++) {
			iva8.add(new IntValue(i));
		}
		iva8.addAll(iva8);

		return new IntValueArray[] {iva0, iva1, iva2, iva3, iva4, iva5, iva6, iva7, iva8};
	}
}
