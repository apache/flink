/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValueStateDescriptorTest {
	
	@Test
	public void testValueStateDescriptorEagerSerializer() throws Exception {

		TypeSerializer<String> serializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		String defaultValue = "le-value-default";
		
		ValueStateDescriptor<String> descr = 
				new ValueStateDescriptor<String>("testName", serializer, defaultValue);
		
		assertEquals("testName", descr.getName());
		assertEquals(defaultValue, descr.getDefaultValue());
		assertNotNull(descr.getSerializer());
		assertEquals(serializer, descr.getSerializer());

		ValueStateDescriptor<String> copy = CommonTestUtils.createCopySerializable(descr);

		assertEquals("testName", copy.getName());
		assertEquals(defaultValue, copy.getDefaultValue());
		assertNotNull(copy.getSerializer());
		assertEquals(serializer, copy.getSerializer());
	}

	@Test
	public void testValueStateDescriptorLazySerializer() throws Exception {
		
		// some default value that goes to the generic serializer
		Path defaultValue = new Path(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).toURI());
		
		// some different registered value
		ExecutionConfig cfg = new ExecutionConfig();
		cfg.registerKryoType(TaskInfo.class);

		ValueStateDescriptor<Path> descr =
				new ValueStateDescriptor<Path>("testName", Path.class, defaultValue);

		try {
			descr.getSerializer();
			fail("should cause an exception");
		} catch (IllegalStateException ignored) {}

		descr.initializeSerializerUnlessSet(cfg);
		
		assertNotNull(descr.getSerializer());
		assertTrue(descr.getSerializer() instanceof KryoSerializer);

		assertTrue(((KryoSerializer<?>) descr.getSerializer()).getKryo().getRegistration(TaskInfo.class).getId() > 0);
	}

	@Test
	public void testValueStateDescriptorAutoSerializer() throws Exception {
		
		String defaultValue = "le-value-default";

		ValueStateDescriptor<String> descr =
				new ValueStateDescriptor<String>("testName", String.class, defaultValue);

		ValueStateDescriptor<String> copy = CommonTestUtils.createCopySerializable(descr);

		assertEquals("testName", copy.getName());
		assertEquals(defaultValue, copy.getDefaultValue());
		assertNotNull(copy.getSerializer());
		assertEquals(StringSerializer.INSTANCE, copy.getSerializer());
	}

	@Test
	public void testVeryLargeDefaultValue() throws Exception {
		// ensure that we correctly read very large data when deserializing the default value

		TypeSerializer<String> serializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		byte[] data = new byte[200000];
		for (int i = 0; i < 200000; i++) {
			data[i] = 65;
		}
		data[199000] = '\0';

		String defaultValue = new String(data);

		ValueStateDescriptor<String> descr =
				new ValueStateDescriptor<String>("testName", serializer, defaultValue);

		assertEquals("testName", descr.getName());
		assertEquals(defaultValue, descr.getDefaultValue());
		assertNotNull(descr.getSerializer());
		assertEquals(serializer, descr.getSerializer());

		ValueStateDescriptor<String> copy = CommonTestUtils.createCopySerializable(descr);

		assertEquals("testName", copy.getName());
		assertEquals(defaultValue, copy.getDefaultValue());
		assertNotNull(copy.getSerializer());
		assertEquals(serializer, copy.getSerializer());
	}
}
