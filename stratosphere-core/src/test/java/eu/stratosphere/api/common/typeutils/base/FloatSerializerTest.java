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
package eu.stratosphere.api.common.typeutils.base;

import java.util.Random;

import eu.stratosphere.api.common.typeutils.SerializerTestBase;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
/**
 * A test for the {@link FloatSerializerTest}.
 */
public class FloatSerializerTest extends SerializerTestBase<Float> {
	
	@Override
	protected TypeSerializer<Float> createSerializer() {
		return new FloatSerializer();
	}
	
	@Override
	protected int getLength() {
		return 4;
	}
	
	@Override
	protected Class<Float> getTypeClass() {
		return Float.class;
	}
	
	@Override
	protected Float[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		float rndFloat = rnd.nextFloat() * Float.MAX_VALUE;
		
		return new Float[] {new Float(0), new Float(1), new Float(-1),
							new Float(Float.MAX_VALUE), new Float(Float.MIN_VALUE),
							new Float(rndFloat), new Float(-rndFloat),
							new Float(Float.NaN),
							new Float(Float.NEGATIVE_INFINITY), new Float(Float.POSITIVE_INFINITY)};
	}
}	