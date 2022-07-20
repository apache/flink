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

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for {@link ResourceInformationReflector}. */
class ResourceInformationReflectorTest {

    private static final String RESOURCE_NAME = "test";
    private static final long RESOURCE_VALUE = 1;

    @Test
    void testSetResourceInformationIfMethodPresent() {
        final ResourceInformationReflector resourceInformationReflector =
                new ResourceInformationReflector(
                        ResourceWithMethod.class.getName(), ResourceInfoWithMethod.class.getName());
        final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
        resourceInformationReflector.setResourceInformationUnSafe(
                resourceWithMethod, RESOURCE_NAME, RESOURCE_VALUE);

        assertThat(resourceWithMethod.getResourceWithName(RESOURCE_NAME)).isNotNull();
        assertThat(resourceWithMethod.getResourceWithName(RESOURCE_NAME).getName())
                .isEqualTo(RESOURCE_NAME);
        assertThat(resourceWithMethod.getResourceWithName(RESOURCE_NAME).getValue())
                .isEqualTo(RESOURCE_VALUE);
    }

    @Test
    void testGetResourceInformationIfMethodPresent() {
        final ResourceInformationReflector resourceInformationReflector =
                new ResourceInformationReflector(
                        ResourceWithMethod.class.getName(), ResourceInfoWithMethod.class.getName());
        final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
        resourceWithMethod.setResourceInformation(
                RESOURCE_NAME, ResourceInfoWithMethod.newInstance(RESOURCE_NAME, RESOURCE_VALUE));

        final Map<String, Long> externalResources =
                resourceInformationReflector.getExternalResourcesUnSafe(resourceWithMethod);
        assertThat(externalResources).hasSize(1).containsEntry(RESOURCE_NAME, RESOURCE_VALUE);
    }

    @Test
    void testSetResourceInformationIfMethodAbsent() {
        final ResourceInformationReflector resourceInformationReflector =
                new ResourceInformationReflector(
                        ResourceWithoutMethod.class.getName(),
                        ResourceInfoWithMethod.class.getName());
        final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
        resourceInformationReflector.setResourceInformationUnSafe(
                resourceWithMethod, RESOURCE_NAME, RESOURCE_VALUE);

        assertThat(resourceWithMethod.getResourceWithName(RESOURCE_NAME)).isNull();
    }

    @Test
    void testGetResourceInformationIfMethodAbsent() {
        final ResourceInformationReflector resourceInformationReflector =
                new ResourceInformationReflector(
                        ResourceWithoutMethod.class.getName(),
                        ResourceInfoWithMethod.class.getName());
        final ResourceWithMethod resourceWithMethod = new ResourceWithMethod();
        resourceWithMethod.setResourceInformation(
                RESOURCE_NAME, ResourceInfoWithMethod.newInstance(RESOURCE_NAME, RESOURCE_VALUE));

        final Map<String, Long> externalResources =
                resourceInformationReflector.getExternalResourcesUnSafe(resourceWithMethod);
        assertThat(externalResources.entrySet()).isEmpty();
    }

    @Test
    void testDefaultTwoResourceTypeWithYarnSupport() {
        assumeThat(HadoopUtils.isMinHadoopVersion(2, 10)).isTrue();

        final Resource resource = Resource.newInstance(100, 1);

        // make sure that Resource has at least two associated resources (cpu and memory)
        final Map<String, Long> resourcesResult =
                ResourceInformationReflector.INSTANCE.getAllResourceInfos(resource);
        assertThat(resourcesResult).hasSizeGreaterThanOrEqualTo(2);
    }

    @Test
    void testSetAndGetExtendedResourcesWithoutYarnSupport() {
        assumeThat(HadoopUtils.isMaxHadoopVersion(2, 10)).isTrue();

        final Resource resource = Resource.newInstance(100, 1);

        // Should do nothing without leading to failure.
        ResourceInformationReflector.INSTANCE.setResourceInformation(
                resource, RESOURCE_NAME, RESOURCE_VALUE);

        final Map<String, Long> externalResourcesResult =
                ResourceInformationReflector.INSTANCE.getExternalResources(resource);
        assertThat(externalResourcesResult).isEmpty();
    }

    /**
     * Class which has methods with the same signature as {@link
     * org.apache.hadoop.yarn.api.records.Resource} in Hadoop 2.10+ and 3.0+.
     */
    private static class ResourceWithMethod {
        private final Map<String, ResourceInfoWithMethod> externalResource = new HashMap<>();

        public void setResourceInformation(
                String name, ResourceInfoWithMethod resourceInfoWithMethod) {
            externalResource.put(name, resourceInfoWithMethod);
        }

        public ResourceInfoWithMethod[] getResources() {
            ResourceInfoWithMethod[] resourceInfos =
                    new ResourceInfoWithMethod[2 + externalResource.size()];
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
     * Class which has methods with the same signature as {@link
     * org.apache.hadoop.yarn.api.records.ResourceInformation} in Hadoop 2.10+ and 3.0+.
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

    /** Class which does not has required methods. */
    private static class ResourceWithoutMethod {}
}
