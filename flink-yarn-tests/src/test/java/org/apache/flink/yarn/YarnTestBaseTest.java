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

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.assertj.core.api.Fail.fail;

/** Tests for {@link YarnTestBase}. */
class YarnTestBaseTest {

    @Test
    void ensureWhitelistEntryMatches() {
        ensureWhitelistEntryMatch("465 java.lang.InterruptedException: sleep interrupted");
        ensureWhitelistEntryMatch(
                "2020-09-19 22:06:19,458 WARN  org.apache.pekko.remote.ReliableDeliverySupervisor                       [] - Association with remote system [pekko.tcp://flink@e466f3e261f3:42352] has failed, address is now gated for [50] ms. Reason: [Association failed with [pekko.tcp://flink@e466f3e261f3:42352]] Caused by: [java.net.ConnectException: Connection refused: e466f3e261f3/192.168.224.2:42352]");
        ensureWhitelistEntryMatch(
                "2026-06-18 05:08:21,447 WARN  org.apache.pekko.remote.ReliableDeliverySupervisor           [] - Association with remote system [pekko.tcp://flink@14b2224d67fd:38335] has failed, address is now gated for [50] ms. Reason: [Association failed with [pekko.tcp://flink@14b2224d67fd:38335]] Caused by: [org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: 14b2224d67fd/172.21.0.2:38335, caused by: java.net.ConnectException: Connection refused]");
        ensureWhitelistEntryMatch(
                "2020-10-15 10:31:09,661 WARN  org.apache.pekko.remote.transport.netty.NettyTransport                   [] - Remote connection to [61b81e62b514/192.168.128.2:39365] failed with java.io.IOException: Broken pipe");
    }

    @Test
    void ensureGatedReassociationWithNonConnectionCauseIsNotWhitelisted() {
        ensureWhitelistEntryDoesNotMatch(
                "2026-06-18 05:08:21,447 WARN  org.apache.pekko.remote.ReliableDeliverySupervisor           [] - Association with remote system [pekko.tcp://flink@14b2224d67fd:38335] has failed, address is now gated for [50] ms. Reason: [Association failed with [pekko.tcp://flink@14b2224d67fd:38335]] Caused by: [java.lang.IllegalStateException: something unrelated]");
        ensureWhitelistEntryDoesNotMatch(
                "2026-06-18 05:08:21,447 WARN  org.apache.pekko.remote.ReliableDeliverySupervisor           [] - Association with remote system [pekko.tcp://flink@14b2224d67fd:38335] has failed, address is now gated for [50] ms. Reason: [Association failed with [pekko.tcp://flink@14b2224d67fd:38335]] Caused by: [java.lang.NullPointerException: Connection refused: 14b2224d67fd/172.21.0.2:38335]");
    }

    private void ensureWhitelistEntryMatch(String probe) {
        for (Pattern pattern : YarnTestBase.WHITELISTED_STRINGS) {
            if (pattern.matcher(probe).find()) {
                return;
            }
        }
        fail("The following string didn't match any whitelisted patterns '" + probe + "'");
    }

    private void ensureWhitelistEntryDoesNotMatch(String probe) {
        for (Pattern pattern : YarnTestBase.WHITELISTED_STRINGS) {
            if (pattern.matcher(probe).find()) {
                fail(
                        "The following string unexpectedly matched whitelisted pattern '"
                                + pattern
                                + "': '"
                                + probe
                                + "'");
            }
        }
    }
}
