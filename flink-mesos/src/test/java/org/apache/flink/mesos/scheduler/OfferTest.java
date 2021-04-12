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

package org.apache.flink.mesos.scheduler;

import org.apache.flink.util.TestLogger;

import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.mesos.Utils.UNRESERVED_ROLE;
import static org.apache.flink.mesos.Utils.cpus;
import static org.apache.flink.mesos.Utils.disk;
import static org.apache.flink.mesos.Utils.gpus;
import static org.apache.flink.mesos.Utils.mem;
import static org.apache.flink.mesos.Utils.network;
import static org.apache.flink.mesos.Utils.ports;
import static org.apache.flink.mesos.Utils.range;
import static org.apache.flink.mesos.Utils.resources;
import static org.apache.flink.mesos.Utils.scalar;

/** Tests {@link Offer} which adapts a Mesos offer as a lease for use with Fenzo. */
public class OfferTest extends TestLogger {

    private static final double EPSILON = 1e-5;

    private static final Protos.FrameworkID FRAMEWORK_ID =
            Protos.FrameworkID.newBuilder().setValue("framework-1").build();
    private static final Protos.OfferID OFFER_ID =
            Protos.OfferID.newBuilder().setValue("offer-1").build();
    private static final String HOSTNAME = "host-1";
    private static final Protos.SlaveID AGENT_ID =
            Protos.SlaveID.newBuilder().setValue("agent-1").build();

    private static final String ROLE_A = "A";

    private static final String ATTR_1 = "A1";

    // region Resources

    /** Tests basic properties (other than those of specific resources, covered elsewhere). */
    @Test
    public void testResourceProperties() {
        Offer offer = new Offer(offer(resources(), attrs()));
        Assert.assertNotNull(offer.getResources());
        Assert.assertEquals(HOSTNAME, offer.hostname());
        Assert.assertEquals(AGENT_ID.getValue(), offer.getVMID());
        Assert.assertNotNull(offer.getOffer());
        Assert.assertEquals(OFFER_ID.getValue(), offer.getId());
        Assert.assertNotEquals(0L, offer.getOfferedTime());
        Assert.assertNotNull(offer.getAttributeMap());
        Assert.assertNotNull(offer.toString());
    }

    /** Tests aggregation of resources in the presence of unreserved plus reserved resources. */
    @Test
    public void testResourceAggregation() {
        Offer offer;

        offer = new Offer(offer(resources(), attrs()));
        Assert.assertEquals(0.0, offer.cpuCores(), EPSILON);
        Assert.assertEquals(Arrays.asList(), ranges(offer.portRanges()));

        offer =
                new Offer(
                        offer(
                                resources(
                                        cpus(ROLE_A, 1.0),
                                        cpus(UNRESERVED_ROLE, 1.0),
                                        ports(ROLE_A, range(80, 80), range(443, 444)),
                                        ports(UNRESERVED_ROLE, range(8080, 8081)),
                                        otherScalar(42.0)),
                                attrs()));
        Assert.assertEquals(2.0, offer.cpuCores(), EPSILON);
        Assert.assertEquals(
                Arrays.asList(range(80, 80), range(443, 444), range(8080, 8081)),
                ranges(offer.portRanges()));
    }

    @Test
    public void testCpuCores() {
        Offer offer = new Offer(offer(resources(cpus(1.0)), attrs()));
        Assert.assertEquals(1.0, offer.cpuCores(), EPSILON);
    }

    @Test
    public void testGPUs() {
        Offer offer = new Offer(offer(resources(gpus(1.0)), attrs()));
        Assert.assertEquals(1.0, offer.gpus(), EPSILON);
    }

    @Test
    public void testMemoryMB() {
        Offer offer = new Offer(offer(resources(mem(1024.0)), attrs()));
        Assert.assertEquals(1024.0, offer.memoryMB(), EPSILON);
    }

    @Test
    public void testNetworkMbps() {
        Offer offer = new Offer(offer(resources(network(10.0)), attrs()));
        Assert.assertEquals(10.0, offer.networkMbps(), EPSILON);
    }

    @Test
    public void testDiskMB() {
        Offer offer = new Offer(offer(resources(disk(1024.0)), attrs()));
        Assert.assertEquals(1024.0, offer.diskMB(), EPSILON);
    }

    @Test
    public void testPortRanges() {
        Offer offer = new Offer(offer(resources(ports(range(8080, 8081))), attrs()));
        Assert.assertEquals(
                Collections.singletonList(range(8080, 8081)), ranges(offer.portRanges()));
    }

    // endregion

    // region Attributes

    @Test
    public void testAttributeIndexing() {
        Offer offer = new Offer(offer(resources(), attrs(attr(ATTR_1, 42.0))));
        Assert.assertEquals(attr(ATTR_1, 42.0), offer.getAttributeMap().get(ATTR_1));
    }

    // endregion

    // region Utilities

    private static Protos.Offer offer(
            List<Protos.Resource> resources, List<Protos.Attribute> attributes) {
        return Protos.Offer.newBuilder()
                .setId(OFFER_ID)
                .setFrameworkId(FRAMEWORK_ID)
                .setHostname(HOSTNAME)
                .setSlaveId(AGENT_ID)
                .addAllAttributes(attributes)
                .addAllResources(resources)
                .build();
    }

    private static Protos.Attribute attr(String name, double scalar) {
        return Protos.Attribute.newBuilder()
                .setName(name)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(scalar))
                .build();
    }

    private static List<Protos.Attribute> attrs(Protos.Attribute... attributes) {
        return Arrays.asList(attributes);
    }

    private static List<Protos.Value.Range> ranges(List<VirtualMachineLease.Range> ranges) {
        return ranges.stream()
                .map(
                        r ->
                                Protos.Value.Range.newBuilder()
                                        .setBegin(r.getBeg())
                                        .setEnd(r.getEnd())
                                        .build())
                .collect(Collectors.toList());
    }

    private static Protos.Resource otherScalar(double value) {
        return scalar("mem", UNRESERVED_ROLE, value);
    }

    // endregion
}
