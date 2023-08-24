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

package org.apache.flink.runtime.security;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_KEYTAB;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_PRINCIPAL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SecurityConfiguration}. */
class SecurityConfigurationTest {

    @Test
    void keytabWithoutPrincipalShouldThrowException() {
        Configuration configuration = new Configuration();
        configuration.set(KERBEROS_LOGIN_KEYTAB, "keytab.file");

        assertThatThrownBy(() -> new SecurityConfiguration(configuration))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(
                        "either both keytab and principal must be defined, or neither");
    }

    @Test
    void principalWithoutKeytabShouldThrowException() {
        Configuration configuration = new Configuration();
        configuration.set(KERBEROS_LOGIN_PRINCIPAL, "principal");

        assertThatThrownBy(() -> new SecurityConfiguration(configuration))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(
                        "either both keytab and principal must be defined, or neither");
    }

    @Test
    void notExistingKeytabShouldThrowException() {
        Configuration configuration = new Configuration();
        configuration.set(KERBEROS_LOGIN_KEYTAB, "nonexistingkeytab.file");
        configuration.set(KERBEROS_LOGIN_PRINCIPAL, "principal");

        assertThatThrownBy(() -> new SecurityConfiguration(configuration))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("nonexistingkeytab.file")
                .hasMessageContaining("doesn't exist");
    }
}
