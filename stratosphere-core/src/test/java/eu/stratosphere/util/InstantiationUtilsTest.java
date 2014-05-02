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
package eu.stratosphere.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class InstantiationUtilsTest {

	@Test
	public void testInstatiationOfStringValue() {
		StringValue stringValue = InstantiationUtil.instantiate(
				StringValue.class, null);
		assertNotNull(stringValue);
	}

	@Test
	public void testInstatiationOfStringValueAndCastToValue() {
		StringValue stringValue = InstantiationUtil.instantiate(
				StringValue.class, Value.class);
		assertNotNull(stringValue);
	}

	@Test
	public void testHasNullaryConstructor() {
		assertTrue(InstantiationUtil
				.hasPublicNullaryConstructor(StringValue.class));
	}

	@Test
	public void testClassIsProper() {
		assertTrue(InstantiationUtil.isProperClass(StringValue.class));
	}

	@Test
	public void testClassIsNotProper() {
		assertFalse(InstantiationUtil.isProperClass(Value.class));
	}

	@Test(expected = RuntimeException.class)
	public void testCheckForInstantiationOfPrivateClass() {
		InstantiationUtil.checkForInstantiation(TestClass.class);
	}

	private class TestClass {

	}
}
