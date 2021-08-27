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

package org.apache.flink.test.state;

import org.apache.flink.test.state.BackendSwitchSpecs.BackendSwitchSpec;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/** Tests for switching a HEAP state backend to a different one. */
@RunWith(Parameterized.class)
public class HeapSavepointStateBackendSwitchTest extends SavepointStateBackendSwitchTestBase {
    public HeapSavepointStateBackendSwitchTest(BackendSwitchSpec toBackend) {
        super(BackendSwitchSpecs.HEAP, toBackend);
    }

    @Parameterized.Parameters(name = "Target Backend: {0}")
    public static Collection<BackendSwitchSpec> targetBackends() {
        return Arrays.asList(BackendSwitchSpecs.HEAP, BackendSwitchSpecs.ROCKS);
    }
}
