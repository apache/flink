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
 * A test for the {@link IntSerializerTest}.
 */
public class IntSerializerTest extends SerializerTestBase<Integer> {
	
	@Override
	protected TypeSerializer<Integer> createSerializer() {
		return new IntSerializer();
	}
	
	@Override
	protected int getLength() {
		return 4;
	}
	
	@Override
	protected Class<Integer> getTypeClass() {
		return Integer.class;
	}
	
	@Override
	protected Integer[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		int rndInt = rnd.nextInt();
		
		return new Integer[] {new Integer(0), new Integer(1), new Integer(-1),
							new Integer(Integer.MAX_VALUE), new Integer(Integer.MIN_VALUE),
							new Integer(rndInt), new Integer(-rndInt)};
	}
}	
