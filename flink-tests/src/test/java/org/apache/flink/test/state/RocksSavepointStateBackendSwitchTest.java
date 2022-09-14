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
import java.util.List;
import java.util.stream.Collectors;

/** Tests for switching a RocksDB state backend to a different one. */
@RunWith(Parameterized.class)
public class RocksSavepointStateBackendSwitchTest extends SavepointStateBackendSwitchTestBase {
    public RocksSavepointStateBackendSwitchTest(
            BackendSwitchSpec fromBackend, BackendSwitchSpec toBackend) {
        super(fromBackend, toBackend);
    }

    @Parameterized.Parameters(name = "from: {0} to: {1}")
    public static Collection<BackendSwitchSpec[]> targetBackends() {
        List<BackendSwitchSpec> fromBackends =
                Arrays.asList(BackendSwitchSpecs.ROCKS_HEAP_TIMERS, BackendSwitchSpecs.ROCKS);
        List<BackendSwitchSpec> toBackends =
                Arrays.asList(
                        BackendSwitchSpecs.HEAP,
                        BackendSwitchSpecs.ROCKS,
                        BackendSwitchSpecs.ROCKS_HEAP_TIMERS);
        return fromBackends.stream()
                .flatMap(from -> toBackends.stream().map(to -> new BackendSwitchSpec[] {from, to}))
                .collect(Collectors.toList());
    }
}
