/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** {@link LocalChangelogRegistryImpl}'s test. */
public class LocalChangelogRegistryTest extends TestLogger {

    @Test
    public void testRegistryNormal() {
        LocalChangelogRegistry localStateRegistry =
                new LocalChangelogRegistryImpl(Executors.directExecutor());
        TestingStreamStateHandle handle1 = new TestingStreamStateHandle();
        TestingStreamStateHandle handle2 = new TestingStreamStateHandle();
        // checkpoint 1: handle1, handle2
        localStateRegistry.register(handle1, 1);
        localStateRegistry.register(handle2, 1);

        // checkpoint 2: handle2, handle3
        TestingStreamStateHandle handle3 = new TestingStreamStateHandle();
        localStateRegistry.register(handle2, 2);
        localStateRegistry.register(handle3, 2);

        localStateRegistry.discardUpToCheckpoint(2);
        assertTrue(handle1.isDisposed());
        assertFalse(handle2.isDisposed());

        localStateRegistry.discardUpToCheckpoint(3);
        assertTrue(handle2.isDisposed());
        assertTrue(handle3.isDisposed());
    }
}
