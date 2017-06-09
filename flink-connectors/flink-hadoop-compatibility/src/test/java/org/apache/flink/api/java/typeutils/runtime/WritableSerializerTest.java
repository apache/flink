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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;

import org.junit.Test;

/**
 * Tests for the {@link WritableSerializer}.
 */
public class WritableSerializerTest {

	@Test
	public void testStringArrayWritable() {
		StringArrayWritable[] data = new StringArrayWritable[]{
				new StringArrayWritable(new String[]{}),
				new StringArrayWritable(new String[]{""}),
				new StringArrayWritable(new String[]{"a", "a"}),
				new StringArrayWritable(new String[]{"a", "b"}),
				new StringArrayWritable(new String[]{"c", "c"}),
				new StringArrayWritable(new String[]{"d", "f"}),
				new StringArrayWritable(new String[]{"d", "m"}),
				new StringArrayWritable(new String[]{"z", "x"}),
				new StringArrayWritable(new String[]{"a", "a", "a"})
		};

		WritableTypeInfo<StringArrayWritable> writableTypeInfo = (WritableTypeInfo<StringArrayWritable>) TypeExtractor.getForObject(data[0]);
		WritableSerializer<StringArrayWritable> writableSerializer = (WritableSerializer<StringArrayWritable>) writableTypeInfo.createSerializer(new ExecutionConfig());

		SerializerTestInstance<StringArrayWritable> testInstance = new SerializerTestInstance<StringArrayWritable>(writableSerializer, writableTypeInfo.getTypeClass(), -1, data);

		testInstance.testAll();
	}
}
