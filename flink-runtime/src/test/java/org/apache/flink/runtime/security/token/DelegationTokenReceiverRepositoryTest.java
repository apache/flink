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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DelegationTokenReceiverRepository}. */
class DelegationTokenReceiverRepositoryTest {

    @BeforeEach
    void beforeEach() {
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @AfterEach
    void afterEach() {
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @Test
    void configurationIsNullMustFailFast() {
        assertThatThrownBy(() -> new DelegationTokenReceiverRepository(null, null))
                .isInstanceOf(Exception.class);
    }

    @Test
    void oneReceiverThrowsExceptionMustFailFast() {
        assertThatThrownBy(
                        () -> {
                            ExceptionThrowingDelegationTokenReceiver.throwInInit.set(true);
                            new DelegationTokenReceiverRepository(new Configuration(), null);
                        })
                .isInstanceOf(Exception.class);
    }

    @Test
    void testAllReceiversLoaded() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".throw.enabled", false);
        DelegationTokenReceiverRepository delegationTokenReceiverRepository =
                new DelegationTokenReceiverRepository(configuration, null);

        assertThat(delegationTokenReceiverRepository.delegationTokenReceivers).hasSize(3);
        assertThat(delegationTokenReceiverRepository.isReceiverLoaded("hadoopfs")).isTrue();
        assertThat(delegationTokenReceiverRepository.isReceiverLoaded("hbase")).isTrue();
        assertThat(delegationTokenReceiverRepository.isReceiverLoaded("test")).isTrue();
        assertThat(ExceptionThrowingDelegationTokenReceiver.constructed.get()).isTrue();
        assertThat(delegationTokenReceiverRepository.isReceiverLoaded("throw")).isFalse();
    }
}
