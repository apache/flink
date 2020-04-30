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
import org.apache.flink.types.DoubleValue;

/**
 * A test for the {@link DoubleValueSerializer}.
 */
public class DoubleValueSerializerTest extends SerializerTestBase<DoubleValue> {
	
	@Override
	protected TypeSerializer<DoubleValue> createSerializer() {
		return new DoubleValueSerializer();
	}
	
	@Override
	protected int getLength() {
		return 8;
	}
	
	@Override
	protected Class<DoubleValue> getTypeClass() {
		return DoubleValue.class;
	}
	
	@Override
	protected DoubleValue[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		Double rndDouble = rnd.nextDouble() * Double.MAX_VALUE;
		
		return new DoubleValue[] {new DoubleValue(0), new DoubleValue(1), new DoubleValue(-1),
							new DoubleValue(Double.MAX_VALUE), new DoubleValue(Double.MIN_VALUE),
							new DoubleValue(rndDouble), new DoubleValue(-rndDouble),
							new DoubleValue(Double.NaN),
							new DoubleValue(Double.NEGATIVE_INFINITY), new DoubleValue(Double.POSITIVE_INFINITY)};
	}
}	
