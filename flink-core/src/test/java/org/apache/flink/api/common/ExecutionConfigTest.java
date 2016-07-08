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

package org.apache.flink.api.common;

import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExecutionConfigTest {

	@Test
	public void testDoubleTypeRegistration() {
		ExecutionConfig config = new ExecutionConfig();
		List<Class<?>> types = Arrays.<Class<?>>asList(Double.class, Integer.class, Double.class);
		List<Class<?>> expectedTypes = Arrays.<Class<?>>asList(Double.class, Integer.class);

		for(Class<?> tpe: types) {
			config.registerKryoType(tpe);
		}

		int counter = 0;

		for(Class<?> tpe: config.getRegisteredKryoTypes()){
			assertEquals(tpe, expectedTypes.get(counter++));
		}

		assertTrue(counter == expectedTypes.size());
	}

	@Test
	public void testConfigurationOfParallelism() {
		ExecutionConfig config = new ExecutionConfig();

		// verify explicit change in parallelism
		int parallelism = 36;
		config.setParallelism(parallelism);

		assertEquals(parallelism, config.getParallelism());

		// verify that parallelism is reset to default flag value
		parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
		config.setParallelism(parallelism);

		assertEquals(parallelism, config.getParallelism());
	}

	/**
	 * Helper function to create a new ExecutionConfig for tests.
	 * @return A serialized ExecutionConfig
	 */
	public static SerializedValue<ExecutionConfig> getSerializedConfig() {
		try {
			return new SerializedValue<>(new ExecutionConfig());
		} catch (IOException e) {
			throw new RuntimeException("Couldn't create new ExecutionConfig for test.", e);
		}
	}
}
