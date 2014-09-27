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
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
/**
 * A test for the {@link DoubleSerializerTest}.
 */
public class DoubleSerializerTest extends SerializerTestBase<Double> {
	
	@Override
	protected TypeSerializer<Double> createSerializer() {
		return new DoubleSerializer();
	}
	
	@Override
	protected int getLength() {
		return 8;
	}
	
	@Override
	protected Class<Double> getTypeClass() {
		return Double.class;
	}
	
	@Override
	protected Double[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		Double rndDouble = rnd.nextDouble() * Double.MAX_VALUE;
		
		return new Double[] {new Double(0), new Double(1), new Double(-1),
							new Double(Double.MAX_VALUE), new Double(Double.MIN_VALUE),
							new Double(rndDouble), new Double(-rndDouble),
							new Double(Double.NaN),
							new Double(Double.NEGATIVE_INFINITY), new Double(Double.POSITIVE_INFINITY)};
	}
}	
