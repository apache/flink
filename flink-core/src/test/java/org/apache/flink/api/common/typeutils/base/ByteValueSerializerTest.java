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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.ByteValue;

/**
 * A test for the {@link ByteValueSerializer}.
 */
public class ByteValueSerializerTest extends SerializerTestBase<ByteValue> {
	
	@Override
	protected TypeSerializer<ByteValue> createSerializer() {
		return new ByteValueSerializer();
	}
	
	@Override
	protected int getLength() {
		return 1;
	}
	
	@Override
	protected Class<ByteValue> getTypeClass() {
		return ByteValue.class;
	}
	
	@Override
	protected ByteValue[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		byte byteArray[] = new byte[1];
		rnd.nextBytes(byteArray);
		
		return new ByteValue[] {new ByteValue((byte) 0), new ByteValue((byte) 1), new ByteValue((byte) -1), 
							new ByteValue(Byte.MAX_VALUE), new ByteValue(Byte.MIN_VALUE),
							new ByteValue(byteArray[0]), new ByteValue((byte) -byteArray[0])};
	}
}
