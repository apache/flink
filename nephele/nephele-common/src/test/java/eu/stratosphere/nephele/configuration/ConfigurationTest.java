/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class contains test for the configuration package. In particular, the serialization of {@link Configuration}
 * objects is tested.
 * 
 * @author casp
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

		final Configuration copy = (Configuration) CommonTestUtils.createCopy(orig);

		assertEquals(copy.getString("mykey", "null"), "myvalue");
		assertEquals(copy.getBoolean("shouldbetrue", false), true);
		assertEquals(copy.getInteger("mynumber", 0), 100);
		assertEquals(copy.getClass("myclass", null).toString(), this.getClass().toString());
		assertTrue(orig.equals(copy));
		assertTrue(orig.keySet().equals(copy.keySet()));
	}
}
