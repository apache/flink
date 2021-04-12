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

package org.apache.flink.runtime.management;

import org.junit.Test;

import java.net.ServerSocket;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for the singleton usage via {@link JMXService}. */
public class JMXServiceTest {

    /** Verifies initialize with port range. */
    @Test
    public void testJMXServiceInit() throws Exception {
        try {
            JMXService.startInstance("23456-23466");
            assertTrue(JMXService.getPort().isPresent());
        } finally {
            JMXService.stopInstance();
        }
    }

    /** Verifies initialize failure with occupied port. */
    @Test
    public void testJMXServiceInitWithOccupiedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            JMXService.startInstance(String.valueOf(socket.getLocalPort()));
            assertFalse(JMXService.getInstance().isPresent());
        }
    }
}
