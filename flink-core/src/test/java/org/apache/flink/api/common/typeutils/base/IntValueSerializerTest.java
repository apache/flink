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
import org.apache.flink.types.IntValue;

/**
 * A test for the {@link IntValueSerializer}.
 */
public class IntValueSerializerTest extends SerializerTestBase<IntValue> {
	
	@Override
	protected TypeSerializer<IntValue> createSerializer() {
		return new IntValueSerializer();
	}
	
	@Override
	protected int getLength() {
		return 4;
	}
	
	@Override
	protected Class<IntValue> getTypeClass() {
		return IntValue.class;
	}
	
	@Override
	protected IntValue[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		int rndInt = rnd.nextInt();
		
		return new IntValue[] {new IntValue(0), new IntValue(1), new IntValue(-1),
							new IntValue(Integer.MAX_VALUE), new IntValue(Integer.MIN_VALUE),
							new IntValue(rndInt), new IntValue(-rndInt)};
	}
}	
