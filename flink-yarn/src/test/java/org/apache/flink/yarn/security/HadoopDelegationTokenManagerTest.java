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

package org.apache.flink.yarn.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link HadoopDelegationTokenManager}. */
public class HadoopDelegationTokenManagerTest {

    private final org.apache.flink.configuration.Configuration flinkConf =
            new org.apache.flink.configuration.Configuration();

    private final Configuration hadoopConf = new Configuration();

    private final HadoopDelegationTokenConfiguration hadoopDelegationTokenConf =
            new HadoopDelegationTokenConfiguration(flinkConf, hadoopConf);

    @Test
    public void testProvidersLoadedNormally() {

        HadoopDelegationTokenManager manager =
                new HadoopDelegationTokenManager(hadoopDelegationTokenConf);

        // built-in providers:
        assertTrue(manager.isProviderLoaded("hadoopfs"));
        assertTrue(manager.isProviderLoaded("hbase"));
    }

    @Test
    public void testObtainDelegationTokens() {
        HadoopDelegationTokenManager manager =
                new HadoopDelegationTokenManager(hadoopDelegationTokenConf);

        Credentials credentials = new Credentials();
        manager.obtainDelegationTokens(credentials);

        // In default configuration, there should no tokens obtained
        assertEquals(0, credentials.numberOfTokens());
        assertEquals(0, credentials.numberOfSecretKeys());
    }
}
