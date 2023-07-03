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

package org.apache.flink.queryablestate.client.proxy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.KvStateLocationOracle;
import org.apache.flink.runtime.query.KvStateLocation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link KvStateClientProxyImpl}. */
class KvStateClientProxyImplTest {

    private KvStateClientProxyImpl kvStateClientProxy;

    @BeforeEach
    void setup() {
        kvStateClientProxy =
                new KvStateClientProxyImpl(
                        InetAddress.getLoopbackAddress().getHostName(),
                        Collections.singleton(0).iterator(),
                        1,
                        1,
                        new DisabledKvStateRequestStats());
    }

    @AfterEach
    void shutdown() {
        kvStateClientProxy.shutdown();
    }

    /** Tests that we can set and retrieve the {@link KvStateLocationOracle}. */
    @Test
    void testKvStateLocationOracle() {
        final JobID jobId1 = new JobID();
        final TestingKvStateLocationOracle kvStateLocationOracle1 =
                new TestingKvStateLocationOracle();
        kvStateClientProxy.updateKvStateLocationOracle(jobId1, kvStateLocationOracle1);
        final JobID jobId2 = new JobID();
        final TestingKvStateLocationOracle kvStateLocationOracle2 =
                new TestingKvStateLocationOracle();
        kvStateClientProxy.updateKvStateLocationOracle(jobId2, kvStateLocationOracle2);

        assertThat(kvStateClientProxy.getKvStateLocationOracle(new JobID())).isNull();

        assertThat(kvStateClientProxy.getKvStateLocationOracle(jobId1))
                .isEqualTo(kvStateLocationOracle1);
        assertThat(kvStateClientProxy.getKvStateLocationOracle(jobId2))
                .isEqualTo(kvStateLocationOracle2);

        kvStateClientProxy.updateKvStateLocationOracle(jobId1, null);
        assertThat(kvStateClientProxy.getKvStateLocationOracle(jobId1)).isNull();
    }

    /**
     * Tests that {@link KvStateLocationOracle} registered under {@link
     * HighAvailabilityServices#DEFAULT_JOB_ID} will be used for all requests.
     */
    @Test
    void testLegacyCodePathPreference() {
        final TestingKvStateLocationOracle kvStateLocationOracle =
                new TestingKvStateLocationOracle();
        kvStateClientProxy.updateKvStateLocationOracle(
                HighAvailabilityServices.DEFAULT_JOB_ID, kvStateLocationOracle);
        final JobID jobId = new JobID();
        kvStateClientProxy.updateKvStateLocationOracle(jobId, new TestingKvStateLocationOracle());

        assertThat(kvStateClientProxy.getKvStateLocationOracle(jobId))
                .isEqualTo(kvStateLocationOracle);
    }

    /** Testing implementation of {@link KvStateLocationOracle}. */
    private static final class TestingKvStateLocationOracle implements KvStateLocationOracle {

        @Override
        public CompletableFuture<KvStateLocation> requestKvStateLocation(
                JobID jobId, String registrationName) {
            return null;
        }
    }
}
