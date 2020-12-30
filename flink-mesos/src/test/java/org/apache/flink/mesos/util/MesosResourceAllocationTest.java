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

package org.apache.flink.mesos.util;

import org.apache.flink.util.TestLogger;

import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.flink.mesos.Utils.UNRESERVED_ROLE;
import static org.apache.flink.mesos.Utils.cpus;
import static org.apache.flink.mesos.Utils.ports;
import static org.apache.flink.mesos.Utils.range;
import static org.apache.flink.mesos.Utils.resources;

/** Tests {@link MesosResourceAllocation}. */
public class MesosResourceAllocationTest extends TestLogger {

    // possible roles
    private static final String ROLE_A = "A";
    private static final String ROLE_B = "B";

    // possible framework configurations
    private static final Set<String> AS_ROLE_A = Collections.singleton(ROLE_A);
    private static final Set<String> AS_NO_ROLE = Collections.emptySet();

    // region Reservations

    /** Tests that reserved resources are prioritized. */
    @Test
    public void testReservationPrioritization() {
        MesosResourceAllocation allocation =
                new MesosResourceAllocation(
                        resources(
                                cpus(ROLE_A, 1.0), cpus(UNRESERVED_ROLE, 1.0), cpus(ROLE_B, 1.0)));
        Assert.assertEquals(
                resources(cpus(ROLE_A, 1.0), cpus(ROLE_B, 1.0), cpus(UNRESERVED_ROLE, 1.0)),
                allocation.getRemaining());
    }

    /** Tests that resources are filtered according to the framework role (if any). */
    @Test
    public void testReservationFiltering() {
        MesosResourceAllocation allocation;

        // unreserved resources
        allocation =
                new MesosResourceAllocation(
                        resources(
                                cpus(UNRESERVED_ROLE, 1.0), ports(UNRESERVED_ROLE, range(80, 80))));
        Assert.assertEquals(
                resources(cpus(UNRESERVED_ROLE, 1.0)),
                allocation.takeScalar("cpus", 1.0, AS_NO_ROLE));
        Assert.assertEquals(
                resources(ports(UNRESERVED_ROLE, range(80, 80))),
                allocation.takeRanges("ports", 1, AS_NO_ROLE));
        allocation =
                new MesosResourceAllocation(
                        resources(
                                cpus(UNRESERVED_ROLE, 1.0), ports(UNRESERVED_ROLE, range(80, 80))));
        Assert.assertEquals(
                resources(cpus(UNRESERVED_ROLE, 1.0)),
                allocation.takeScalar("cpus", 1.0, AS_ROLE_A));
        Assert.assertEquals(
                resources(ports(UNRESERVED_ROLE, range(80, 80))),
                allocation.takeRanges("ports", 1, AS_ROLE_A));

        // reserved for the framework role
        allocation =
                new MesosResourceAllocation(
                        resources(cpus(ROLE_A, 1.0), ports(ROLE_A, range(80, 80))));
        Assert.assertEquals(resources(), allocation.takeScalar("cpus", 1.0, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.takeRanges("ports", 1, AS_NO_ROLE));
        Assert.assertEquals(
                resources(cpus(ROLE_A, 1.0)), allocation.takeScalar("cpus", 1.0, AS_ROLE_A));
        Assert.assertEquals(
                resources(ports(ROLE_A, range(80, 80))),
                allocation.takeRanges("ports", 1, AS_ROLE_A));

        // reserved for a different role
        allocation =
                new MesosResourceAllocation(
                        resources(cpus(ROLE_B, 1.0), ports(ROLE_B, range(80, 80))));
        Assert.assertEquals(resources(), allocation.takeScalar("cpus", 1.0, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.takeRanges("ports", 1, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.takeScalar("cpus", 1.0, AS_ROLE_A));
        Assert.assertEquals(resources(), allocation.takeRanges("ports", 1, AS_ROLE_A));
    }

    // endregion

    // region General

    /** Tests resource naming and typing. */
    @Test
    public void testResourceSpecificity() {
        MesosResourceAllocation allocation =
                new MesosResourceAllocation(resources(cpus(1.0), ports(range(80, 80))));

        // mismatched name
        Assert.assertEquals(resources(), allocation.takeScalar("other", 1.0, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.takeRanges("other", 1, AS_NO_ROLE));

        // mismatched type
        Assert.assertEquals(resources(), allocation.takeScalar("ports", 1.0, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.takeRanges("cpus", 1, AS_NO_ROLE));

        // nothing lost
        Assert.assertEquals(resources(cpus(1.0), ports(range(80, 80))), allocation.getRemaining());
    }

    // endregion

    // region Scalar Resources

    /** Tests scalar resource accounting. */
    @Test
    public void testScalarResourceAccounting() {
        MesosResourceAllocation allocation;

        // take part of a resource
        allocation = new MesosResourceAllocation(resources(cpus(1.0)));
        Assert.assertEquals(resources(cpus(0.25)), allocation.takeScalar("cpus", 0.25, AS_NO_ROLE));
        Assert.assertEquals(resources(cpus(0.75)), allocation.getRemaining());

        // take a whole resource
        allocation = new MesosResourceAllocation(resources(cpus(1.0)));
        Assert.assertEquals(resources(cpus(1.0)), allocation.takeScalar("cpus", 1.0, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.getRemaining());

        // take multiple resources
        allocation =
                new MesosResourceAllocation(
                        resources(cpus(ROLE_A, 1.0), cpus(UNRESERVED_ROLE, 1.0)));
        Assert.assertEquals(
                resources(cpus(ROLE_A, 1.0), cpus(UNRESERVED_ROLE, 0.25)),
                allocation.takeScalar("cpus", 1.25, AS_ROLE_A));
        Assert.assertEquals(resources(cpus(UNRESERVED_ROLE, 0.75)), allocation.getRemaining());
    }

    /** Tests scalar resource exhaustion (i.e. insufficient resources). */
    @Test
    public void testScalarResourceExhaustion() {
        MesosResourceAllocation allocation = new MesosResourceAllocation(resources(cpus(1.0)));
        Assert.assertEquals(resources(cpus(1.0)), allocation.takeScalar("cpus", 2.0, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.getRemaining());
    }

    // endregion

    // region Range Resources

    /** Tests range resource accounting. */
    @Test
    public void testRangeResourceAccounting() {
        MesosResourceAllocation allocation;
        List<Protos.Resource> ports =
                resources(
                        ports(ROLE_A, range(80, 81), range(443, 444)),
                        ports(UNRESERVED_ROLE, range(1024, 1025), range(8080, 8081)));

        // take a partial range of one resource
        allocation = new MesosResourceAllocation(ports);
        Assert.assertEquals(
                resources(ports(ROLE_A, range(80, 80))),
                allocation.takeRanges("ports", 1, AS_ROLE_A));
        Assert.assertEquals(
                resources(
                        ports(ROLE_A, range(81, 81), range(443, 444)),
                        ports(UNRESERVED_ROLE, range(1024, 1025), range(8080, 8081))),
                allocation.getRemaining());

        // take a whole range of one resource
        allocation = new MesosResourceAllocation(ports);
        Assert.assertEquals(
                resources(ports(ROLE_A, range(80, 81))),
                allocation.takeRanges("ports", 2, AS_ROLE_A));
        Assert.assertEquals(
                resources(
                        ports(ROLE_A, range(443, 444)),
                        ports(UNRESERVED_ROLE, range(1024, 1025), range(8080, 8081))),
                allocation.getRemaining());

        // take numerous ranges of one resource
        allocation = new MesosResourceAllocation(ports);
        Assert.assertEquals(
                resources(ports(ROLE_A, range(80, 81), range(443, 443))),
                allocation.takeRanges("ports", 3, AS_ROLE_A));
        Assert.assertEquals(
                resources(
                        ports(ROLE_A, range(444, 444)),
                        ports(UNRESERVED_ROLE, range(1024, 1025), range(8080, 8081))),
                allocation.getRemaining());

        // take a whole resource
        allocation = new MesosResourceAllocation(ports);
        Assert.assertEquals(
                resources(ports(ROLE_A, range(80, 81), range(443, 444))),
                allocation.takeRanges("ports", 4, AS_ROLE_A));
        Assert.assertEquals(
                resources(ports(UNRESERVED_ROLE, range(1024, 1025), range(8080, 8081))),
                allocation.getRemaining());

        // take numerous resources
        allocation = new MesosResourceAllocation(ports);
        Assert.assertEquals(
                resources(
                        ports(ROLE_A, range(80, 81), range(443, 444)),
                        ports(UNRESERVED_ROLE, range(1024, 1024))),
                allocation.takeRanges("ports", 5, AS_ROLE_A));
        Assert.assertEquals(
                resources(ports(UNRESERVED_ROLE, range(1025, 1025), range(8080, 8081))),
                allocation.getRemaining());
    }

    /** Tests range resource exhaustion (i.e. insufficient resources). */
    @Test
    public void testRangeResourceExhaustion() {
        MesosResourceAllocation allocation =
                new MesosResourceAllocation(resources(ports(range(80, 80))));
        Assert.assertEquals(
                resources(ports(range(80, 80))), allocation.takeRanges("ports", 2, AS_NO_ROLE));
        Assert.assertEquals(resources(), allocation.getRemaining());
    }

    // endregion
}
