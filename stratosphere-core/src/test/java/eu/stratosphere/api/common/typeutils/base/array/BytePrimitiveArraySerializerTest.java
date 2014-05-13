/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.common.typeutils.base.array;

import java.util.Random;

import eu.stratosphere.api.common.typeutils.SerializerTestBase;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.LongPrimitiveArraySerializer;

/**
 * A test for the {@link LongPrimitiveArraySerializer}.
 */
public class BytePrimitiveArraySerializerTest extends SerializerTestBase<byte[]> {

	private final Random rnd = new Random(346283764872L);
	
	@Override
	protected TypeSerializer<byte[]> createSerializer() {
		return new BytePrimitiveArraySerializer();
	}

	@Override
	protected Class<byte[]> getTypeClass() {
		return byte[].class;
	}
	
	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected byte[][] getTestData() {
		return new byte[][] {
			randomByteArray(),
			randomByteArray(),
			new byte[] {},
			randomByteArray(),
			randomByteArray(),
			randomByteArray(),
			new byte[] {},
			randomByteArray(),
			randomByteArray(),
			randomByteArray(),
			new byte[] {}
		};
	}
	
	private final byte[] randomByteArray() {
		int len = rnd.nextInt(1024 * 1024);
		byte[] data = new byte[len];
		for (int i = 0; i < len; i++) {
			data[i] = (byte) rnd.nextInt();
		}
		return data;
	}
}
