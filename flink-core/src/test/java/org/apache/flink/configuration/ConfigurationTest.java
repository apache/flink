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


package org.apache.flink.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.junit.Test;

/**
 * This class contains test for the configuration package. In particular, the serialization of {@link Configuration}
 * objects is tested.
 */
public class ConfigurationTest {

	/**
	 * This test checks the serialization/deserialization of configuration objects.
	 */
	@Test
	public void testConfigurationSerialization() {

		// First, create initial configuration object with some parameters
		final Configuration orig = new Configuration();
		orig.setString("mykey", "myvalue");
		orig.setBoolean("shouldbetrue", true);
		orig.setInteger("mynumber", 100);
		orig.setClass("myclass", this.getClass());

		try {
			final Configuration copy = (Configuration) CommonTestUtils.createCopy(orig);

			assertEquals(copy.getString("mykey", "null"), "myvalue");
			assertEquals(copy.getBoolean("shouldbetrue", false), true);
			assertEquals(copy.getInteger("mynumber", 0), 100);
			assertEquals(copy.getClass("myclass", null).toString(), this.getClass().toString());
			assertTrue(orig.equals(copy));
			assertTrue(orig.keySet().equals(copy.keySet()));

		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
}
