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

package org.apache.flink.tests.util.activation;

import org.apache.flink.util.OperatingSystem;

import org.junit.Assume;
import org.junit.AssumptionViolatedException;

import java.util.Arrays;
import java.util.EnumSet;

/**
 * Utility for tests/resources to restrict test execution to certain operating systems.
 *
 * <p>Do not call these methods in a static initializer block as this marks the test as failed.
 */
public enum OperatingSystemRestriction {
    ;

    /**
     * Restricts the execution to the given set of operating systems.
     *
     * @param reason reason for the restriction
     * @param operatingSystems allowed operating systems
     * @throws AssumptionViolatedException if this method is called on a forbidden operating system
     */
    public static void restrictTo(final String reason, final OperatingSystem... operatingSystems)
            throws AssumptionViolatedException {
        final EnumSet<OperatingSystem> allowed = EnumSet.copyOf(Arrays.asList(operatingSystems));
        Assume.assumeTrue(reason, allowed.contains(OperatingSystem.getCurrentOperatingSystem()));
    }

    /**
     * Forbids the execution on the given set of operating systems.
     *
     * @param reason reason for the restriction
     * @param forbiddenSystems forbidden operating systems
     * @throws AssumptionViolatedException if this method is called on a forbidden operating system
     */
    public static void forbid(final String reason, final OperatingSystem... forbiddenSystems)
            throws AssumptionViolatedException {
        final OperatingSystem os = OperatingSystem.getCurrentOperatingSystem();
        for (final OperatingSystem forbiddenSystem : forbiddenSystems) {
            Assume.assumeTrue(reason, os != forbiddenSystem);
        }
    }
}
