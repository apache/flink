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

package org.apache.flink.runtime.rpc;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.util.TestLogger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Validate the given {@link CloseableRegistry} in test case. */
public abstract class EndpointCloseableRegistryTest extends TestLogger {
    /**
     * Validate the registered closeable resource count of the given {@link CloseableRegistry}.
     *
     * @param registeredCount registered closeable resource count in given {@link CloseableRegistry}
     * @param closeableRegistry the given {@link CloseableRegistry}
     */
    protected void validateRegisteredResourceCount(
            int registeredCount, CloseableRegistry closeableRegistry) {
        assertFalse(closeableRegistry.isClosed());
        assertEquals(registeredCount, closeableRegistry.getNumberOfRegisteredCloseables());
    }

    /**
     * Validate if the given {@link CloseableRegistry} is closed.
     *
     * @param closeableRegistry the given {@link CloseableRegistry}
     */
    protected void validateRegistryClosed(CloseableRegistry closeableRegistry) {
        assertTrue(closeableRegistry.isClosed());
        assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());
    }
}
