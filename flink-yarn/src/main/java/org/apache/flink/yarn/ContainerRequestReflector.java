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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Use reflection to determine whether the Hadoop supports node-label, depending on the Hadoop
 * version, may or may not be supported. If not, nothing happened.
 *
 * <p>The node label mechanism is supported by Hadoop version greater than 2.6.0
 */
class ContainerRequestReflector {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerRequestReflector.class);

    static final ContainerRequestReflector INSTANCE = new ContainerRequestReflector();

    @Nullable private Constructor<? extends AMRMClient.ContainerRequest> defaultConstructor;

    private ContainerRequestReflector() {
        this(AMRMClient.ContainerRequest.class);
    }

    @VisibleForTesting
    ContainerRequestReflector(Class<? extends AMRMClient.ContainerRequest> containerRequestClass) {
        Class<? extends AMRMClient.ContainerRequest> requestCls = containerRequestClass;
        try {
            /**
             * To support node-label, using the below constructor. Please refer to
             * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/AMRMClient.java#L287
             *
             * <p>Instantiates a {@link ContainerRequest} with the given constraints.
             *
             * @param capability The {@link Resource} to be requested for each container.
             * @param nodes Any hosts to request that the containers are placed on.
             * @param racks Any racks to request that the containers are placed on. The racks
             *     corresponding to any hosts requested will be automatically added to this list.
             * @param priority The priority at which to request the containers. Higher priorities
             *     have lower numerical values.
             * @param allocationRequestId The allocationRequestId of the request. To be used as a
             *     tracking id to match Containers allocated against this request. Will default to 0
             *     if not specified.
             * @param relaxLocality If true, containers for this request may be assigned on hosts
             *     and racks other than the ones explicitly requested.
             * @param nodeLabelsExpression Set node labels to allocate resource, now we only support
             *     asking for only a single node label
             */
            defaultConstructor =
                    requestCls.getDeclaredConstructor(
                            Resource.class,
                            String[].class,
                            String[].class,
                            Priority.class,
                            boolean.class,
                            String.class);
        } catch (NoSuchMethodException exception) {
            LOG.debug(
                    "The node-label mechanism of Yarn don't be supported in this Hadoop version.");
        }
    }

    public AMRMClient.ContainerRequest getContainerRequest(
            Resource containerResource, Priority priority, String nodeLabel) {
        if (StringUtils.isNullOrWhitespaceOnly(nodeLabel) || defaultConstructor == null) {
            return new AMRMClient.ContainerRequest(containerResource, null, null, priority);
        }

        try {
            /**
             * Set the param of relaxLocality to true, which tells the Yarn ResourceManager if the
             * application wants locality to be loose (i.e. allows fall-through to rack or any)
             */
            return defaultConstructor.newInstance(
                    containerResource, null, null, priority, true, nodeLabel);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            LOG.warn("Errors on creating Container Request.", e);
        }

        return new AMRMClient.ContainerRequest(containerResource, null, null, priority);
    }
}
