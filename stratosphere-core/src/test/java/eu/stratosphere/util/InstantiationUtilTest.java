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

import eu.stratosphere.api.common.typeutils.base.DoubleValueSerializer;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.TypeInformation;
import junit.framework.Assert;
import org.junit.Test;

import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

import java.io.IOException;
import java.util.Random;

public class InstantiationUtilTest {

	@Test
	public void testInstatiationOfStringValue() {
		StringValue stringValue = InstantiationUtil.instantiate(StringValue.class, null);
		assertNotNull(stringValue);
	}

	@Test
	public void testInstatiationOfStringValueAndCastToValue() {
		StringValue stringValue = InstantiationUtil.instantiate(StringValue.class, Value.class);
		assertNotNull(stringValue);
	}

	@Test
	public void testHasNullaryConstructor() {
		assertTrue(InstantiationUtil.hasPublicNullaryConstructor(StringValue.class));
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

	@Test
	public void testSerializationToByteArray() throws IOException {
		final DoubleValue toSerialize = new DoubleValue(Math.random());
		final DoubleValueSerializer serializer = new DoubleValueSerializer();

		byte[] serialized = InstantiationUtil.serializeToByteArray(serializer, toSerialize);

		DoubleValue deserialized = InstantiationUtil.deserializeFromByteArray(serializer, serialized);

		Assert.assertEquals("Serialized record is not equal after serialization.", toSerialize, deserialized);
	}

	private class TestClass {}
}
