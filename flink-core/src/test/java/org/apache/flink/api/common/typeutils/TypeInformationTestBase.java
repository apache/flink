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

package org.apache.flink.api.common.typeutils;

import java.io.IOException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Abstract test base for type information.
 */
public abstract class TypeInformationTestBase<T extends TypeInformation<?>> extends TestLogger {

	protected abstract T[] getTestData();

	@Test
	public void testHashcodeAndEquals() throws Exception {
		final T[] testData = getTestData();
		final TypeInformation<?> unrelatedTypeInfo = new UnrelatedTypeInfo();

		for (T typeInfo : testData) {
			// check for implemented hashCode and equals
			if (typeInfo.getClass().getMethod("hashCode").getDeclaringClass() == Object.class) {
				throw new AssertionError("Type information does not implement own hashCode method: " +
					typeInfo.getClass().getCanonicalName());
			}
			if (typeInfo.getClass().getMethod("equals", Object.class).getDeclaringClass() == Object.class) {
				throw new AssertionError("Type information does not implement own equals method: " +
					typeInfo.getClass().getCanonicalName());
			}

			// compare among test data
			for (T otherTypeInfo : testData) {
				assertTrue("canEqual() returns inconsistent results.", typeInfo.canEqual(otherTypeInfo));
				// test equality
				if (typeInfo == otherTypeInfo) {
					assertTrue("hashCode() returns inconsistent results.", typeInfo.hashCode() == otherTypeInfo.hashCode());
					assertEquals("equals() is false for same object.", typeInfo, otherTypeInfo);
				}
				// test inequality
				else {
					assertNotEquals("equals() returned true for different objects.", typeInfo, otherTypeInfo);
				}
			}

			// compare with unrelated type
			assertFalse("Type information allows to compare with unrelated type.", typeInfo.canEqual(unrelatedTypeInfo));
			assertNotEquals(typeInfo, unrelatedTypeInfo);
		}
	}

	@Test
	public void testSerialization() {
		final T[] testData = getTestData();

		for (T typeInfo : testData) {
			final byte[] serialized;
			try {
				serialized = InstantiationUtil.serializeObject(typeInfo);
			} catch (IOException e) {
				throw new AssertionError("Could not serialize type information: " + typeInfo, e);
			}
			final T deserialized;
			try {
				deserialized = InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
			} catch (IOException | ClassNotFoundException e) {
				throw new AssertionError("Could not deserialize type information: " + typeInfo, e);
			}
			if (typeInfo.hashCode() != deserialized.hashCode() || !typeInfo.equals(deserialized)) {
				throw new AssertionError("Deserialized type information differs from original one.");
			}
		}
	}

	@Test
	public void testGetTotalFields() {
		final T[] testData = getTestData();
		for (T typeInfo : testData) {
			assertTrue(
				"Number of total fields must be at least 1",
				typeInfo.getTotalFields() > 0);
		}
	}

	private static class UnrelatedTypeInfo extends TypeInformation<Object> {

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 0;
		}

		@Override
		public int getTotalFields() {
			return 0;
		}

		@Override
		public Class<Object> getTypeClass() {
			return null;
		}

		@Override
		public boolean isKeyType() {
			return false;
		}

		@Override
		public TypeSerializer<Object> createSerializer(ExecutionConfig config) {
			return null;
		}

		@Override
		public String toString() {
			return null;
		}

		@Override
		public boolean equals(Object obj) {
			return false;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean canEqual(Object obj) {
			return false;
		}
	}
}
