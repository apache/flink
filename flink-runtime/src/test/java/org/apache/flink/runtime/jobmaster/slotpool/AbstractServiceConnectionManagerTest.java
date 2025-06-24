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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultDeclareResourceRequirementServiceConnectionManager}. */
class AbstractServiceConnectionManagerTest {

    @Test
    void testIsConnected() {
        AbstractServiceConnectionManager<Object> connectionManager =
                new TestServiceConnectionManager();

        assertThat(connectionManager.isConnected()).isFalse();

        connectionManager.connect(new Object());
        assertThat(connectionManager.isConnected()).isTrue();

        connectionManager.disconnect();
        assertThat(connectionManager.isConnected()).isFalse();

        connectionManager.close();
        assertThat(connectionManager.isConnected()).isFalse();
    }

    @Test
    void testCheckNotClosed() {
        AbstractServiceConnectionManager<Object> connectionManager =
                new TestServiceConnectionManager();

        connectionManager.checkNotClosed();

        connectionManager.connect(new Object());
        connectionManager.checkNotClosed();

        connectionManager.disconnect();
        connectionManager.checkNotClosed();

        connectionManager.close();
        assertThatThrownBy(connectionManager::checkNotClosed)
                .as("checkNotClosed() did not fail for a closed connection manager")
                .isInstanceOf(IllegalStateException.class);
    }

    private static class TestServiceConnectionManager
            extends AbstractServiceConnectionManager<Object> {}
}
