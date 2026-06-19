/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

/**
 * Use reflection to determine whether the {@link AMRMClientAsync} supports the updateBlacklist
 * method. If not, we will try to call updateBlacklist of {@link AMRMClientAsync#client}. If both
 * fail, do nothing.
 */
class AMRMClientAsyncReflector {

    private static final Logger LOG = LoggerFactory.getLogger(AMRMClientAsyncReflector.class);

    static final AMRMClientAsyncReflector INSTANCE = new AMRMClientAsyncReflector();

    private static final String UPDATE_BLOCKLIST_METHOD = "updateBlacklist";

    private static final String SYNC_CLIENT = "client";

    private final Optional<Method> asyncClientUpdateBlocklistMethod;

    private final Optional<Method> syncClientUpdateBlocklistMethod;

    private final Optional<Field> syncClientField;

    private AMRMClientAsyncReflector() {
        this.asyncClientUpdateBlocklistMethod =
                tryGetMethod(
                        AMRMClientAsync.class, UPDATE_BLOCKLIST_METHOD, List.class, List.class);
        this.syncClientUpdateBlocklistMethod =
                tryGetMethod(AMRMClient.class, UPDATE_BLOCKLIST_METHOD, List.class, List.class);
        this.syncClientField = tryGetField(AMRMClientAsync.class, SYNC_CLIENT);
    }

    private static <T> Optional<Method> tryGetMethod(
            Class<T> clazz, String methodName, Class<?>... parameterTypes) {
        try {
            return Optional.of(clazz.getMethod(methodName, parameterTypes));
        } catch (NoSuchMethodException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "{} does not support method {} in this YARN version.",
                        clazz.getCanonicalName(),
                        methodName);
            }
            return Optional.empty();
        }
    }

    private static <T> Optional<Field> tryGetField(Class<T> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return Optional.of(field);
        } catch (NoSuchFieldException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "{} does not have field {} in this YARN version.",
                        clazz.getCanonicalName(),
                        fieldName);
            }
            return Optional.empty();
        }
    }

    public void tryUpdateBlockList(
            AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient,
            List<String> blocklistAdditions,
            List<String> blocklistRemovals) {
        try {
            if (asyncClientUpdateBlocklistMethod.isPresent()) {
                asyncClientUpdateBlocklistMethod
                        .get()
                        .invoke(asyncClient, blocklistAdditions, blocklistRemovals);
            } else if (syncClientUpdateBlocklistMethod.isPresent() && syncClientField.isPresent()) {
                Object syncClient = syncClientField.get().get(asyncClient);
                syncClientUpdateBlocklistMethod
                        .get()
                        .invoke(syncClient, blocklistAdditions, blocklistRemovals);
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Neither %s nor %s support method %s",
                                asyncClient.getClass().getCanonicalName(),
                                syncClientField.isPresent()
                                        ? syncClientField
                                                .get()
                                                .get(asyncClient)
                                                .getClass()
                                                .getCanonicalName()
                                        : null,
                                UPDATE_BLOCKLIST_METHOD));
            }
        } catch (Throwable throwable) {
            LOG.warn(
                    "Failed to update blocklist, blocklistAdditions "
                            + blocklistAdditions
                            + ", blocklistRemovals "
                            + blocklistRemovals,
                    throwable);
        }
    }
}
