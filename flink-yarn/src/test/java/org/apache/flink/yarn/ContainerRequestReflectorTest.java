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

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/** Tests for {@link ContainerRequestReflector}. */
public class ContainerRequestReflectorTest extends TestLogger {

    @Test
    public void testGetContainerRequestIfConstructorPresent() {
        final ContainerRequestReflector containerRequestReflector =
                new ContainerRequestReflector(ContainerRequestWithConstructor.class);
        Resource resource = Resource.newInstance(100, 1);
        Priority priority = Priority.newInstance(1);

        AMRMClient.ContainerRequest containerRequest =
                containerRequestReflector.getContainerRequest(resource, priority, "GPU");
        assertTrue(containerRequest instanceof ContainerRequestWithConstructor);
        ContainerRequestWithConstructor containerRequestWithConstructor =
                (ContainerRequestWithConstructor) containerRequest;
        assertEquals("GPU", containerRequestWithConstructor.getNodeLabelsExpression());

        containerRequest = containerRequestReflector.getContainerRequest(resource, priority, null);
        assertFalse(containerRequest instanceof ContainerRequestWithConstructor);

        containerRequest = containerRequestReflector.getContainerRequest(resource, priority, "");
        assertFalse(containerRequest instanceof ContainerRequestWithConstructor);
    }

    @Test
    public void testGetContainerRequestIfConstructorAbsent() {
        final ContainerRequestReflector containerRequestReflector =
                new ContainerRequestReflector(ContainerRequestWithoutConstructor.class);
        Resource resource = Resource.newInstance(100, 1);
        Priority priority = Priority.newInstance(1);

        AMRMClient.ContainerRequest containerRequest =
                containerRequestReflector.getContainerRequest(resource, priority, "GPU");
        assertFalse(containerRequest instanceof ContainerRequestWithoutConstructor);

        containerRequest = containerRequestReflector.getContainerRequest(resource, priority, null);
        assertFalse(containerRequest instanceof ContainerRequestWithoutConstructor);

        containerRequest = containerRequestReflector.getContainerRequest(resource, priority, "");
        assertFalse(containerRequest instanceof ContainerRequestWithoutConstructor);
    }

    @Test
    public void testGetContainerRequestWithoutYarnSupport() {
        assumeTrue(HadoopUtils.isMaxHadoopVersion(2, 6));

        Resource resource = Resource.newInstance(100, 1);
        Priority priority = Priority.newInstance(1);

        ContainerRequestReflector.INSTANCE.getContainerRequest(resource, priority, "GPU");
        ContainerRequestReflector.INSTANCE.getContainerRequest(resource, priority, null);
        ContainerRequestReflector.INSTANCE.getContainerRequest(resource, priority, "");
    }

    @Test
    public void testGetContainerRequestWithYarnSupport()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        assumeTrue(HadoopUtils.isMinHadoopVersion(2, 6));

        Resource resource = Resource.newInstance(100, 1);
        Priority priority = Priority.newInstance(1);

        AMRMClient.ContainerRequest containerRequest =
                ContainerRequestReflector.INSTANCE.getContainerRequest(resource, priority, "GPU");
        assertEquals("GPU", getNodeLabelExpressionWithReflector(containerRequest));

        containerRequest =
                ContainerRequestReflector.INSTANCE.getContainerRequest(resource, priority, null);
        assertNull(getNodeLabelExpressionWithReflector(containerRequest));

        containerRequest =
                ContainerRequestReflector.INSTANCE.getContainerRequest(resource, priority, "");
        assertNull(getNodeLabelExpressionWithReflector(containerRequest));
    }

    private String getNodeLabelExpressionWithReflector(AMRMClient.ContainerRequest containerRequest)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = containerRequest.getClass().getMethod("getNodeLabelExpression");
        return (String) method.invoke(containerRequest);
    }

    /** Class which does not have required constructor. */
    private static class ContainerRequestWithoutConstructor extends AMRMClient.ContainerRequest {

        public ContainerRequestWithoutConstructor(
                Resource capability, String[] nodes, String[] racks, Priority priority) {
            super(capability, nodes, racks, priority);
        }
    }

    /**
     * Class which has constructor with the same signature as {@link
     * org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest} in Hadoop 2.6+.
     */
    private static class ContainerRequestWithConstructor extends AMRMClient.ContainerRequest {
        private String nodeLabelsExpression;

        public ContainerRequestWithConstructor(
                Resource capability, String[] nodes, String[] racks, Priority priority) {
            super(capability, nodes, racks, priority);
        }

        public ContainerRequestWithConstructor(
                Resource capability,
                String[] nodes,
                String[] racks,
                Priority priority,
                boolean relaxLocality,
                String nodeLabelsExpression) {
            super(capability, nodes, racks, priority);
            this.nodeLabelsExpression = nodeLabelsExpression;
        }

        public String getNodeLabelsExpression() {
            return nodeLabelsExpression;
        }
    }
}
