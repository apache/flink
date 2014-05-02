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
 * A test for the {@link LongSerializerTest}.
 */
public class LongSerializerTest extends SerializerTestBase<Long> {
	
	@Override
	protected TypeSerializer<Long> createSerializer() {
		return new LongSerializer();
	}
	
	@Override
	protected int getLength() {
		return 8;
	}
	
	@Override
	protected Class<Long> getTypeClass() {
		return Long.class;
	}
	
	@Override
	protected Long[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		long rndLong = rnd.nextLong();
		
		return new Long[] {new Long(0L), new Long(1L), new Long(-1L),
							new Long(Long.MAX_VALUE), new Long(Long.MIN_VALUE),
							new Long(rndLong), new Long(-rndLong)};
	}
}	
