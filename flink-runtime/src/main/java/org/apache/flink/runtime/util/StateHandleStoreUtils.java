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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * {@code StateHandleStoreUtils} collects utility methods that might be usefule for any {@link
 * StateHandleStore} implementation.
 */
public class StateHandleStoreUtils {

    /**
     * Serializes the passed {@link StateObject} and discards the state in case of failure.
     *
     * @param stateObject the {@code StateObject} that shall be serialized.
     * @return The serialized version of the passed {@code StateObject}.
     * @throws Exception if an error occurred during the serialization. The corresponding {@code
     *     StateObject} will be discarded in that case.
     */
    public static byte[] serializeOrDiscard(StateObject stateObject) throws Exception {
        try {
            return InstantiationUtil.serializeObject(stateObject);
        } catch (Exception e) {
            try {
                stateObject.discardState();
            } catch (Exception discardException) {
                e.addSuppressed(discardException);
            }

            ExceptionUtils.rethrowException(e);
        }

        // will never happen but is added to please the compiler
        return new byte[0];
    }

    /**
     * Deserializes the passed data into a {@link RetrievableStateHandle}.
     *
     * @param data The data that shall be deserialized.
     * @param <T> The type of data handled by the deserialized {@code RetrievableStateHandle}.
     * @return The {@code RetrievableStateHandle} instance.
     * @throws IOException Any of the usual Input/Output related exceptions.
     * @throws ClassNotFoundException If the data couldn't be deserialized into a {@code
     *     RetrievableStateHandle} referring to the expected type {@code <T>}.
     */
    public static <T extends Serializable> T deserialize(byte[] data)
            throws IOException, ClassNotFoundException {
        return InstantiationUtil.deserializeObject(
                data, Thread.currentThread().getContextClassLoader());
    }
}
