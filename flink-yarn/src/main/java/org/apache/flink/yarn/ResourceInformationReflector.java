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

import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Looks up the methods related to
 * org.apache.hadoop.yarn.api.records.Resource#setResourceInformation. Only supported in Hadoop 3.0+
 * or 2.10+.
 */
class ResourceInformationReflector {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceInformationReflector.class);

    static final ResourceInformationReflector INSTANCE = new ResourceInformationReflector();

    /** Class used to set the extended resource. */
    private static final String RESOURCE_INFO_CLASS =
            "org.apache.hadoop.yarn.api.records.ResourceInformation";

    /** Could be Null iff isYarnResourceTypesAvailable is false. */
    @Nullable private final Method resourceSetResourceInformationMethod;

    /** Could be Null iff isYarnResourceTypesAvailable is false. */
    @Nullable private final Method resourceGetResourcesMethod;

    /** Could be Null iff isYarnResourceTypesAvailable is false. */
    @Nullable private final Method resourceInformationGetNameMethod;

    /** Could be Null iff isYarnResourceTypesAvailable is false. */
    @Nullable private final Method resourceInformationGetValueMethod;

    /** Could be Null iff isYarnResourceTypesAvailable is false. */
    @Nullable private final Method resourceInformationNewInstanceMethod;

    private final boolean isYarnResourceTypesAvailable;

    private ResourceInformationReflector() {
        this(Resource.class.getName(), RESOURCE_INFO_CLASS);
    }

    @VisibleForTesting
    ResourceInformationReflector(String resourceClassName, String resourceInfoClassName) {
        Method resourceSetResourceInformationMethod = null;
        Method resourceGetResourcesMethod = null;
        Method resourceInformationGetNameMethod = null;
        Method resourceInformationGetValueMethod = null;
        Method resourceInformationNewInstanceMethod = null;
        boolean isYarnResourceTypesAvailable = false;
        try {
            final Class<?> resourceClass = Class.forName(resourceClassName);
            final Class<?> resourceInfoClass = Class.forName(resourceInfoClassName);
            resourceSetResourceInformationMethod =
                    resourceClass.getMethod(
                            "setResourceInformation", String.class, resourceInfoClass);
            resourceGetResourcesMethod = resourceClass.getMethod("getResources");
            resourceInformationGetNameMethod = resourceInfoClass.getMethod("getName");
            resourceInformationGetValueMethod = resourceInfoClass.getMethod("getValue");
            resourceInformationNewInstanceMethod =
                    resourceInfoClass.getMethod("newInstance", String.class, long.class);
            isYarnResourceTypesAvailable = true;
        } catch (Exception e) {
            LOG.debug("The underlying Yarn version does not support external resources.", e);
        } finally {
            this.resourceSetResourceInformationMethod = resourceSetResourceInformationMethod;
            this.resourceGetResourcesMethod = resourceGetResourcesMethod;
            this.resourceInformationGetNameMethod = resourceInformationGetNameMethod;
            this.resourceInformationGetValueMethod = resourceInformationGetValueMethod;
            this.resourceInformationNewInstanceMethod = resourceInformationNewInstanceMethod;
            this.isYarnResourceTypesAvailable = isYarnResourceTypesAvailable;
        }
    }

    /** Add the given resourceName and value to the {@link Resource}. */
    void setResourceInformation(Resource resource, String resourceName, long amount) {
        setResourceInformationUnSafe(resource, resourceName, amount);
    }

    /**
     * Same as {@link #setResourceInformation(Resource, String, long)} but allows to pass objects
     * that are not of type {@link Resource}.
     */
    @VisibleForTesting
    void setResourceInformationUnSafe(Object resource, String resourceName, long amount) {
        if (!isYarnResourceTypesAvailable) {
            LOG.info(
                    "Will not request extended resource {} because the used YARN version does not support it.",
                    resourceName);
            return;
        }
        try {
            resourceSetResourceInformationMethod.invoke(
                    resource,
                    resourceName,
                    resourceInformationNewInstanceMethod.invoke(null, resourceName, amount));
        } catch (Exception e) {
            LOG.warn(
                    "Error in setting the external resource {}. Will not request this resource from YARN.",
                    resourceName,
                    e);
        }
    }

    /** Get the name and value of external resources from the {@link Resource}. */
    Map<String, Long> getExternalResources(Resource resource) {
        return getExternalResourcesUnSafe(resource);
    }

    /**
     * Same as {@link #getExternalResources(Resource)} but allows to pass objects that are not of
     * type {@link Resource}.
     */
    @VisibleForTesting
    Map<String, Long> getExternalResourcesUnSafe(Object resource) {
        if (!isYarnResourceTypesAvailable) {
            return Collections.emptyMap();
        }

        final Map<String, Long> externalResources = new HashMap<>();
        final Object[] externalResourcesInfo;
        try {
            externalResourcesInfo = (Object[]) resourceGetResourcesMethod.invoke(resource);
            // The first two element would be cpu and mem.
            for (int i = 2; i < externalResourcesInfo.length; i++) {
                final String name =
                        (String) resourceInformationGetNameMethod.invoke(externalResourcesInfo[i]);
                final long value =
                        (long) resourceInformationGetValueMethod.invoke(externalResourcesInfo[i]);
                externalResources.put(name, value);
            }
        } catch (Exception e) {
            LOG.warn("Could not obtain the external resources supported by the given Resource.", e);
            return Collections.emptyMap();
        }
        return externalResources;
    }

    /** Get the name and value of all resources from the {@link Resource}. */
    @VisibleForTesting
    Map<String, Long> getAllResourceInfos(Object resource) {
        if (!isYarnResourceTypesAvailable) {
            return Collections.emptyMap();
        }

        final Map<String, Long> externalResources = new HashMap<>();
        final Object[] externalResourcesInfo;
        try {
            externalResourcesInfo = (Object[]) resourceGetResourcesMethod.invoke(resource);
            for (int i = 0; i < externalResourcesInfo.length; i++) {
                final String name =
                        (String) resourceInformationGetNameMethod.invoke(externalResourcesInfo[i]);
                final long value =
                        (long) resourceInformationGetValueMethod.invoke(externalResourcesInfo[i]);
                externalResources.put(name, value);
            }
        } catch (Exception e) {
            LOG.warn("Could not obtain the external resources supported by the given Resource.", e);
            return Collections.emptyMap();
        }
        return externalResources;
    }
}
