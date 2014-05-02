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
 * A test for the {@link StringSerializer}.
 */
public class ShortSerializerTest extends SerializerTestBase<Short> {
	
	@Override
	protected TypeSerializer<Short> createSerializer() {
		return new ShortSerializer();
	}
	
	@Override
	protected int getLength() {
		return 2;
	}
	
	@Override
	protected Class<Short> getTypeClass() {
		return Short.class;
	}
	
	@Override
	protected Short[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		int rndInt = rnd.nextInt(32767);
		
		return new Short[] {new Short((short) 0), new Short((short) 1), new Short((short) -1),
							new Short((short) rndInt), new Short((short) -rndInt),
							new Short((short) -32767), new Short((short) 32768)};
	}
}
	
