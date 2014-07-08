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
package eu.stratosphere.api.java.typeutils.runtime;

import org.junit.Test;

import eu.stratosphere.api.common.typeutils.SerializerTestInstance;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.WritableTypeInfo;

public class WritableSerializerTest {
	
	@Test
	public void testStringArrayWritable() {
		StringArrayWritable[] data = new StringArrayWritable[]{
				new StringArrayWritable(new String[]{}),
				new StringArrayWritable(new String[]{""}),
				new StringArrayWritable(new String[]{"a","a"}),
				new StringArrayWritable(new String[]{"a","b"}),
				new StringArrayWritable(new String[]{"c","c"}),
				new StringArrayWritable(new String[]{"d","f"}),
				new StringArrayWritable(new String[]{"d","m"}),
				new StringArrayWritable(new String[]{"z","x"}),
				new StringArrayWritable(new String[]{"a","a", "a"})
		};
		
		WritableTypeInfo<StringArrayWritable> writableTypeInfo = (WritableTypeInfo<StringArrayWritable>) TypeExtractor.getForObject(data[0]);
		WritableSerializer<StringArrayWritable> writableSerializer = (WritableSerializer<StringArrayWritable>) writableTypeInfo.createSerializer();
		
		SerializerTestInstance<StringArrayWritable> testInstance = new SerializerTestInstance<StringArrayWritable>(writableSerializer,writableTypeInfo.getTypeClass(), -1, data);
		
		testInstance.testAll();
	}
	
}
