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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the TaskManagerLocation, which identifies the location and connection information of a
 * TaskManager.
 */
public class TaskManagerLocationTest {

    @Test
    public void testEqualsHashAndCompareTo() {
        try {
            ResourceID resourceID1 = new ResourceID("a");
            ResourceID resourceID2 = new ResourceID("b");
            ResourceID resourceID3 = new ResourceID("c");

            // we mock the addresses to save the times of the reverse name lookups
            InetAddress address1 = mock(InetAddress.class);
            when(address1.getCanonicalHostName()).thenReturn("localhost");
            when(address1.getHostName()).thenReturn("localhost");
            when(address1.getHostAddress()).thenReturn("127.0.0.1");
            when(address1.getAddress()).thenReturn(new byte[] {127, 0, 0, 1});

            InetAddress address2 = mock(InetAddress.class);
            when(address2.getCanonicalHostName()).thenReturn("testhost1");
            when(address2.getHostName()).thenReturn("testhost1");
            when(address2.getHostAddress()).thenReturn("0.0.0.0");
            when(address2.getAddress()).thenReturn(new byte[] {0, 0, 0, 0});

            InetAddress address3 = mock(InetAddress.class);
            when(address3.getCanonicalHostName()).thenReturn("testhost2");
            when(address3.getHostName()).thenReturn("testhost2");
            when(address3.getHostAddress()).thenReturn("192.168.0.1");
            when(address3.getAddress()).thenReturn(new byte[] {(byte) 192, (byte) 168, 0, 1});

            // one == four != two != three
            TaskManagerLocation one = new TaskManagerLocation(resourceID1, address1, 19871);
            TaskManagerLocation two = new TaskManagerLocation(resourceID2, address2, 19871);
            TaskManagerLocation three = new TaskManagerLocation(resourceID3, address3, 10871);
            TaskManagerLocation four = new TaskManagerLocation(resourceID1, address1, 19871);

            assertTrue(one.equals(four));
            assertTrue(!one.equals(two));
            assertTrue(!one.equals(three));
            assertTrue(!two.equals(three));
            assertTrue(!three.equals(four));

            assertTrue(one.compareTo(four) == 0);
            assertTrue(four.compareTo(one) == 0);
            assertTrue(one.compareTo(two) != 0);
            assertTrue(one.compareTo(three) != 0);
            assertTrue(two.compareTo(three) != 0);
            assertTrue(three.compareTo(four) != 0);

            {
                int val = one.compareTo(two);
                assertTrue(two.compareTo(one) == -val);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSerialization() {
        try {
            // without resolved hostname
            {
                TaskManagerLocation original =
                        new TaskManagerLocation(
                                ResourceID.generate(), InetAddress.getByName("1.2.3.4"), 8888);

                TaskManagerLocation serCopy = InstantiationUtil.clone(original);
                assertEquals(original, serCopy);
            }

            // with resolved hostname
            {
                TaskManagerLocation original =
                        new TaskManagerLocation(
                                ResourceID.generate(), InetAddress.getByName("127.0.0.1"), 19871);
                original.getFQDNHostname();

                TaskManagerLocation serCopy = InstantiationUtil.clone(original);
                assertEquals(original, serCopy);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetFQDNHostname() {
        try {
            TaskManagerLocation info1 =
                    new TaskManagerLocation(
                            ResourceID.generate(), InetAddress.getByName("127.0.0.1"), 19871);
            assertNotNull(info1.getFQDNHostname());

            TaskManagerLocation info2 =
                    new TaskManagerLocation(
                            ResourceID.generate(), InetAddress.getByName("1.2.3.4"), 8888);
            assertNotNull(info2.getFQDNHostname());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetHostname0() {
        try {
            InetAddress address = mock(InetAddress.class);
            when(address.getCanonicalHostName()).thenReturn("worker2.cluster.mycompany.com");
            when(address.getHostName()).thenReturn("worker2.cluster.mycompany.com");
            when(address.getHostAddress()).thenReturn("127.0.0.1");

            final TaskManagerLocation info =
                    new TaskManagerLocation(ResourceID.generate(), address, 19871);
            Assert.assertEquals("worker2", info.getHostname());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetHostname1() {
        try {
            InetAddress address = mock(InetAddress.class);
            when(address.getCanonicalHostName()).thenReturn("worker10");
            when(address.getHostName()).thenReturn("worker10");
            when(address.getHostAddress()).thenReturn("127.0.0.1");

            TaskManagerLocation info =
                    new TaskManagerLocation(ResourceID.generate(), address, 19871);
            Assert.assertEquals("worker10", info.getHostname());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetHostname2() {
        try {
            final String addressString = "192.168.254.254";

            // we mock the addresses to save the times of the reverse name lookups
            InetAddress address = mock(InetAddress.class);
            when(address.getCanonicalHostName()).thenReturn("192.168.254.254");
            when(address.getHostName()).thenReturn("192.168.254.254");
            when(address.getHostAddress()).thenReturn("192.168.254.254");
            when(address.getAddress())
                    .thenReturn(new byte[] {(byte) 192, (byte) 168, (byte) 254, (byte) 254});

            TaskManagerLocation info =
                    new TaskManagerLocation(ResourceID.generate(), address, 54152);

            assertNotNull(info.getFQDNHostname());
            assertTrue(info.getFQDNHostname().equals(addressString));

            assertNotNull(info.getHostname());
            assertTrue(info.getHostname().equals(addressString));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testNotRetrieveHostName() {
        InetAddress address = mock(InetAddress.class);
        when(address.getCanonicalHostName()).thenReturn("worker10");
        when(address.getHostName()).thenReturn("worker10");
        when(address.getHostAddress()).thenReturn("127.0.0.1");

        TaskManagerLocation info =
                new TaskManagerLocation(
                        ResourceID.generate(),
                        address,
                        19871,
                        new TaskManagerLocation.IpOnlyHostNameSupplier(address));

        assertNotEquals("worker10", info.getHostname());
        assertNotEquals("worker10", info.getFQDNHostname());
        assertEquals("127.0.0.1", info.getHostname());
        assertEquals("127.0.0.1", info.getFQDNHostname());
    }
}
