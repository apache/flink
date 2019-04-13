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

package org.apache.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_TYPE;

/**
 * Tests for the {@link ExternalCatalogDescriptor} descriptor and
 * {@link ExternalCatalogDescriptorValidator} validator.
 */
public class ExternalCatalogDescriptorTest extends DescriptorTestBase {

	private static final String CATALOG_TYPE_VALUE = "ExternalCatalogDescriptorTest";
	private static final int CATALOG_PROPERTY_VERSION_VALUE = 1;
	private static final String CATALOG_FOO = "foo";
	private static final String CATALOG_FOO_VALUE = "foo-1";

	@Test(expected = ValidationException.class)
	public void testMissingCatalogType() {
		removePropertyAndVerify(descriptors().get(0), CATALOG_TYPE);
	}

	@Test(expected = ValidationException.class)
	public void testMissingFoo() {
		removePropertyAndVerify(descriptors().get(0), CATALOG_FOO);
	}

	@Override
	protected List<Descriptor> descriptors() {
		final Descriptor minimumDesc = new TestExternalCatalogDescriptor(CATALOG_FOO_VALUE);
		return Collections.singletonList(minimumDesc);
	}

	@Override
	protected List<Map<String, String>> properties() {
		final Map<String, String> minimumProps = new HashMap<>();
		minimumProps.put(CATALOG_TYPE, CATALOG_TYPE_VALUE);
		minimumProps.put(CATALOG_PROPERTY_VERSION, "" + CATALOG_PROPERTY_VERSION_VALUE);
		minimumProps.put(CATALOG_FOO, CATALOG_FOO_VALUE);
		return Collections.singletonList(minimumProps);
	}

	@Override
	protected DescriptorValidator validator() {
		return new TestExternalCatalogDescriptorValidator();
	}

	private class TestExternalCatalogDescriptor extends ExternalCatalogDescriptor {
		private String foo;

		public TestExternalCatalogDescriptor(@Nullable String foo) {
			super(CATALOG_TYPE_VALUE, CATALOG_PROPERTY_VERSION_VALUE);
			this.foo = foo;
		}

		@Override
		protected Map<String, String> toCatalogProperties() {
			DescriptorProperties properties = new DescriptorProperties();
			if (foo != null) {
				properties.putString(CATALOG_FOO, foo);
			}
			return properties.asMap();
		}
	}

	private class TestExternalCatalogDescriptorValidator extends ExternalCatalogDescriptorValidator {
		@Override
		public void validate(DescriptorProperties properties) {
			super.validate(properties);
			properties.validateString(CATALOG_FOO, false, 1);
		}
	}
}
