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

package org.apache.flink.management.jmx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.lang.management.ManagementFactory;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link JMXServer} functionality. */
public class JMXServerTest {

    @Before
    public void setUp() throws Exception {
        JMXService.startInstance("23456-23466");
    }

    @After
    public void tearDown() throws Exception {
        JMXService.stopInstance();
    }

    /** Verifies initialize, registered mBean and retrieval via attribute. */
    @Test
    public void testJMXServiceRegisterMBean() throws Exception {
        TestObject testObject = new TestObject();
        ObjectName testObjectName = new ObjectName("org.apache.flink.management", "key", "value");
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        try {
            Optional<JMXServer> server = JMXService.getInstance();
            assertTrue(server.isPresent());
            mBeanServer.registerMBean(testObject, testObjectName);

            JMXServiceURL url =
                    new JMXServiceURL(
                            "service:jmx:rmi://localhost:"
                                    + server.get().getPort()
                                    + "/jndi/rmi://localhost:"
                                    + server.get().getPort()
                                    + "/jmxrmi");
            JMXConnector jmxConn = JMXConnectorFactory.connect(url);
            MBeanServerConnection mbeanConnConn = jmxConn.getMBeanServerConnection();

            assertEquals(1, mbeanConnConn.getAttribute(testObjectName, "Foo"));
            mBeanServer.unregisterMBean(testObjectName);
            try {
                mbeanConnConn.getAttribute(testObjectName, "Foo");
            } catch (Exception e) {
                // expected for unregistered objects.
                assertTrue(e instanceof InstanceNotFoundException);
            }
        } finally {
            JMXService.stopInstance();
        }
    }

    /** Test MBean interface. */
    public interface TestObjectMBean {
        int getFoo();
    }

    /** Test MBean Object. */
    public static class TestObject implements TestObjectMBean {
        private int foo = 1;

        @Override
        public int getFoo() {
            return foo;
        }
    }
}
