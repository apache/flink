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

package org.apache.flink.yarn;

import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link ResourceInformationReflector}.
 */
public class ResourceInformationReflectorTest extends TestLogger {

	private static final String RESOURCE_NAME = "test";
	private static final long RESOURCE_VALUE = 1;

	@Test
	public void testSetResourceInformationIfMethodPresent() {
		final ResourceInformationReflector resourceInformationReflector = new ResourceInformationReflector(ResourceWithMethod.class.getName(), ResourceInfoWithMethod.class.getName());
		final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
		resourceInformationReflector.setResourceInformationUnSafe(resourceWithMethod, RESOURCE_NAME, RESOURCE_VALUE);

		assertNotNull(resourceWithMethod.getResourceWithName(RESOURCE_NAME));
		assertThat(resourceWithMethod.getResourceWithName(RESOURCE_NAME).getName(), is(RESOURCE_NAME));
		assertThat(resourceWithMethod.getResourceWithName(RESOURCE_NAME).getValue(), is(RESOURCE_VALUE));
	}

	@Test
	public void testGetResourceInformationIfMethodPresent() {
		final ResourceInformationReflector resourceInformationReflector = new ResourceInformationReflector(ResourceWithMethod.class.getName(), ResourceInfoWithMethod.class.getName());
		final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
		resourceWithMethod.setResourceInformation(RESOURCE_NAME, ResourceInfoWithMethod.newInstance(RESOURCE_NAME, RESOURCE_VALUE));

		final Map<String, Long> externalResources = resourceInformationReflector.getExternalResourcesUnSafe(resourceWithMethod);
		assertThat(externalResources.size(), is(1));
		assertTrue(externalResources.containsKey(RESOURCE_NAME));
		assertThat(externalResources.get(RESOURCE_NAME), is(RESOURCE_VALUE));
	}

	@Test
	public void testSetResourceInformationIfMethodAbsent() {
		final ResourceInformationReflector resourceInformationReflector = new ResourceInformationReflector(ResourceWithoutMethod.class.getName(), ResourceInfoWithMethod.class.getName());
		final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
		resourceInformationReflector.setResourceInformationUnSafe(resourceWithMethod, RESOURCE_NAME, RESOURCE_VALUE);

		assertNull(resourceWithMethod.getResourceWithName(RESOURCE_NAME));
	}

	@Test
	public void testGetResourceInformationIfMethodAbsent() {
		final ResourceInformationReflector resourceInformationReflector = new ResourceInformationReflector(ResourceWithoutMethod.class.getName(), ResourceInfoWithMethod.class.getName());
		final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
		resourceWithMethod.setResourceInformation(RESOURCE_NAME, ResourceInfoWithMethod.newInstance(RESOURCE_NAME, RESOURCE_VALUE));

		final Map<String, Long> externalResources = resourceInformationReflector.getExternalResourcesUnSafe(resourceWithMethod);
		assertThat(externalResources.entrySet(), is(empty()));
	}

	@Test
	public void testDefaultTwoResourceTypeWithYarnSupport() {
		assumeTrue(HadoopUtils.isMinHadoopVersion(2, 10));

		final Resource resource = Resource.newInstance(100, 1);

		// make sure that Resource has at least two associated resources (cpu and memory)
		final Map<String, Long> resourcesResult = ResourceInformationReflector.INSTANCE.getAllResourceInfos(resource);
		assertThat(resourcesResult.size(), is(2));
	}

	@Test
	public void testSetAndGetExtendedResourcesWithoutYarnSupport() {
		assumeTrue(HadoopUtils.isMaxHadoopVersion(2, 10));

		final Resource resource = Resource.newInstance(100, 1);

		// Should do nothing without leading to failure.
		ResourceInformationReflector.INSTANCE.setResourceInformation(resource, RESOURCE_NAME, RESOURCE_VALUE);

		final Map<String, Long> externalResourcesResult = ResourceInformationReflector.INSTANCE.getExternalResources(resource);
		assertTrue(externalResourcesResult.isEmpty());
	}

	/**
	 * Class which has methods with the same signature as
	 * {@link org.apache.hadoop.yarn.api.records.Resource} in Hadoop 2.10+ and 3.0+.
	 */
	private static class ResourceWithMethod {
		private final Map<String, ResourceInfoWithMethod> externalResource = new HashMap<>();

		public void setResourceInformation(String name, ResourceInfoWithMethod resourceInfoWithMethod) {
			externalResource.put(name, resourceInfoWithMethod);
		}

		public ResourceInfoWithMethod[] getResources() {
			ResourceInfoWithMethod[] resourceInfos = new ResourceInfoWithMethod[2 + externalResource.size()];
			resourceInfos[0] = ResourceInfoWithMethod.newInstance("cpu", 1);
			resourceInfos[1] = ResourceInfoWithMethod.newInstance("memory", 1024);
			int i = 2;
			for (ResourceInfoWithMethod resourceInfoWithMethod : externalResource.values()) {
				resourceInfos[i++] = resourceInfoWithMethod;
			}
			return resourceInfos;
		}

		private ResourceInfoWithMethod getResourceWithName(String name) {
			return externalResource.get(name);
		}
	}

	/**
	 * Class which has methods with the same signature as
	 * {@link org.apache.hadoop.yarn.api.records.ResourceInformation} in Hadoop 2.10+ and 3.0+.
	 */
	private static class ResourceInfoWithMethod {
		private final String name;
		private final long value;

		private ResourceInfoWithMethod(String name, long value) {
			this.name = name;
			this.value = value;
		}

		public String getName() {
			return name;
		}

		public long getValue() {
			return value;
		}

		public static ResourceInfoWithMethod newInstance(String name, long value) {
			return new ResourceInfoWithMethod(name, value);
		}
	}

	/**
	 * Class which does not has required methods.
	 */
	private static class ResourceWithoutMethod {
	}
}
