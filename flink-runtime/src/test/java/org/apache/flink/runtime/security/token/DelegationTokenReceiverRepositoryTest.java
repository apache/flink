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

package org.apache.flink.runtime.security.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DelegationTokenReceiverRepository}. */
class DelegationTokenReceiverRepositoryTest {

    @BeforeEach
    public void beforeEach() {
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @AfterEach
    public void afterEach() {
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @Test
    public void configurationIsNullMustFailFast() {
        assertThrows(Exception.class, () -> new DelegationTokenReceiverRepository(null, null));
    }

    @Test
    public void oneReceiverThrowsExceptionMustFailFast() {
        assertThrows(
                Exception.class,
                () -> {
                    ExceptionThrowingDelegationTokenReceiver.throwInInit.set(true);
                    new DelegationTokenReceiverRepository(new Configuration(), null);
                });
    }

    @Test
    public void testAllReceiversLoaded() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        DelegationTokenReceiverRepository delegationTokenReceiverRepository =
                new DelegationTokenReceiverRepository(configuration, null);

        assertEquals(3, delegationTokenReceiverRepository.delegationTokenReceivers.size());
        assertTrue(delegationTokenReceiverRepository.isReceiverLoaded("hadoopfs"));
        assertTrue(delegationTokenReceiverRepository.isReceiverLoaded("hbase"));
        assertTrue(delegationTokenReceiverRepository.isReceiverLoaded("test"));
        assertTrue(ExceptionThrowingDelegationTokenReceiver.constructed.get());
        assertFalse(delegationTokenReceiverRepository.isReceiverLoaded("throw"));
    }

    @Test
    public void testDelegationTokenDisabled() {
        Configuration configuration = new Configuration();
        configuration.set(SecurityOptions.DELEGATION_TOKENS_ENABLED, false);
        DelegationTokenReceiverRepository delegationTokenReceiverRepository =
                new DelegationTokenReceiverRepository(configuration, null);

        assertEquals(0, delegationTokenReceiverRepository.delegationTokenReceivers.size());
        assertFalse(delegationTokenReceiverRepository.isReceiverLoaded("hadoopfs"));
        assertFalse(delegationTokenReceiverRepository.isReceiverLoaded("hbase"));
        assertFalse(delegationTokenReceiverRepository.isReceiverLoaded("test"));
        assertFalse(ExceptionThrowingDelegationTokenReceiver.constructed.get());
        assertFalse(delegationTokenReceiverRepository.isReceiverLoaded("throw"));
    }
}
