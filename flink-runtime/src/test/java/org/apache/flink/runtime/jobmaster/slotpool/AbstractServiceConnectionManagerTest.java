/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link DefaultDeclareResourceRequirementServiceConnectionManager}. */
public class AbstractServiceConnectionManagerTest extends TestLogger {

    @Test
    public void testIsConnected() {
        AbstractServiceConnectionManager<Object> connectionManager =
                new TestServiceConnectionManager();

        assertThat(connectionManager.isConnected(), is(false));

        connectionManager.connect(new Object());
        assertThat(connectionManager.isConnected(), is(true));

        connectionManager.disconnect();
        assertThat(connectionManager.isConnected(), is(false));

        connectionManager.close();
        assertThat(connectionManager.isConnected(), is(false));
    }

    @Test
    public void testCheckNotClosed() {
        AbstractServiceConnectionManager<Object> connectionManager =
                new TestServiceConnectionManager();

        connectionManager.checkNotClosed();

        connectionManager.connect(new Object());
        connectionManager.checkNotClosed();

        connectionManager.disconnect();
        connectionManager.checkNotClosed();

        connectionManager.close();
        try {
            connectionManager.checkNotClosed();
            fail("checkNotClosed() did not fail for a closed connection manager");
        } catch (IllegalStateException expected) {
        }
    }

    private static class TestServiceConnectionManager
            extends AbstractServiceConnectionManager<Object> {}
}
