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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SettableLeaderRetrievalService}. */
class SettableLeaderRetrievalServiceTest {

    private SettableLeaderRetrievalService settableLeaderRetrievalService;

    @BeforeEach
    void setUp() {
        settableLeaderRetrievalService = new SettableLeaderRetrievalService();
    }

    @Test
    void testNotifyListenerLater() throws Exception {
        final String localhost = "localhost";
        settableLeaderRetrievalService.notifyListener(
                localhost, HighAvailabilityServices.DEFAULT_LEADER_ID);

        final TestingListener listener = new TestingListener();
        settableLeaderRetrievalService.start(listener);

        listener.waitForNewLeader();
        assertThat(listener.getAddress()).isEqualTo(localhost);
        assertThat(listener.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
    }

    @Test
    void testNotifyListenerImmediately() throws Exception {
        final TestingListener listener = new TestingListener();
        settableLeaderRetrievalService.start(listener);

        final String localhost = "localhost";
        settableLeaderRetrievalService.notifyListener(
                localhost, HighAvailabilityServices.DEFAULT_LEADER_ID);

        listener.waitForNewLeader();
        assertThat(listener.getAddress()).isEqualTo(localhost);
        assertThat(listener.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
    }
}
