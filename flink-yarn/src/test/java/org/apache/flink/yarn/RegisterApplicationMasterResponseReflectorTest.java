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

import org.apache.flink.util.TestLogger;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.yarn.YarnTestUtils.isHadoopVersionGreaterThanOrEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/** Tests for {@link RegisterApplicationMasterResponseReflector}. */
public class RegisterApplicationMasterResponseReflectorTest extends TestLogger {

    private static final Logger LOG =
            LoggerFactory.getLogger(RegisterApplicationMasterResponseReflectorTest.class);

    @Mock private Container mockContainer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCallsGetContainersFromPreviousAttemptsMethodIfPresent() {
        final RegisterApplicationMasterResponseReflector
                registerApplicationMasterResponseReflector =
                        new RegisterApplicationMasterResponseReflector(LOG, HasMethod.class);

        final List<Container> containersFromPreviousAttemptsUnsafe =
                registerApplicationMasterResponseReflector.getContainersFromPreviousAttemptsUnsafe(
                        new HasMethod());

        assertThat(containersFromPreviousAttemptsUnsafe, hasSize(1));
    }

    @Test
    public void testDoesntCallGetContainersFromPreviousAttemptsMethodIfAbsent() {
        final RegisterApplicationMasterResponseReflector
                registerApplicationMasterResponseReflector =
                        new RegisterApplicationMasterResponseReflector(LOG, HasMethod.class);

        final List<Container> containersFromPreviousAttemptsUnsafe =
                registerApplicationMasterResponseReflector.getContainersFromPreviousAttemptsUnsafe(
                        new Object());

        assertThat(containersFromPreviousAttemptsUnsafe, empty());
    }

    @Test
    public void testGetContainersFromPreviousAttemptsMethodReflectiveHadoop22() {
        assumeTrue(
                "Method getContainersFromPreviousAttempts is not supported by Hadoop: "
                        + VersionInfo.getVersion(),
                isHadoopVersionGreaterThanOrEquals(2, 2));

        final RegisterApplicationMasterResponseReflector
                registerApplicationMasterResponseReflector =
                        new RegisterApplicationMasterResponseReflector(LOG);

        assertTrue(
                registerApplicationMasterResponseReflector
                        .getGetContainersFromPreviousAttemptsMethod()
                        .isPresent());
    }

    @Test
    public void testCallsGetSchedulerResourceTypesMethodIfPresent() {
        final RegisterApplicationMasterResponseReflector
                registerApplicationMasterResponseReflector =
                        new RegisterApplicationMasterResponseReflector(LOG, HasMethod.class);

        final Optional<Set<String>> schedulerResourceTypeNames =
                registerApplicationMasterResponseReflector.getSchedulerResourceTypeNamesUnsafe(
                        new HasMethod());

        assertTrue(schedulerResourceTypeNames.isPresent());
        assertThat(schedulerResourceTypeNames.get(), containsInAnyOrder("MEMORY", "CPU"));
    }

    @Test
    public void testDoesntCallGetSchedulerResourceTypesMethodIfAbsent() {
        final RegisterApplicationMasterResponseReflector
                registerApplicationMasterResponseReflector =
                        new RegisterApplicationMasterResponseReflector(LOG, HasMethod.class);

        final Optional<Set<String>> schedulerResourceTypeNames =
                registerApplicationMasterResponseReflector.getSchedulerResourceTypeNamesUnsafe(
                        new Object());

        assertFalse(schedulerResourceTypeNames.isPresent());
    }

    @Test
    public void testGetSchedulerResourceTypesMethodReflectiveHadoop26() {
        assumeTrue(
                "Method getSchedulerResourceTypes is not supported by Hadoop: "
                        + VersionInfo.getVersion(),
                isHadoopVersionGreaterThanOrEquals(2, 6));

        final RegisterApplicationMasterResponseReflector
                registerApplicationMasterResponseReflector =
                        new RegisterApplicationMasterResponseReflector(LOG);

        assertTrue(
                registerApplicationMasterResponseReflector
                        .getGetSchedulerResourceTypesMethod()
                        .isPresent());
    }

    /**
     * Class which has a method with the same signature as {@link
     * RegisterApplicationMasterResponse#getContainersFromPreviousAttempts()}.
     */
    private class HasMethod {

        /** Called from {@link #testCallsGetContainersFromPreviousAttemptsMethodIfPresent()}. */
        @SuppressWarnings("unused")
        public List<Container> getContainersFromPreviousAttempts() {
            return Collections.singletonList(mockContainer);
        }

        /** Called from {@link #testCallsGetSchedulerResourceTypesMethodIfPresent()}. */
        @SuppressWarnings("unused")
        public EnumSet<MockSchedulerResourceTypes> getSchedulerResourceTypes() {
            return EnumSet.allOf(MockSchedulerResourceTypes.class);
        }
    }

    @SuppressWarnings("unused")
    private enum MockSchedulerResourceTypes {
        MEMORY,
        CPU
    }
}
