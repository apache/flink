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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.PipelineOptions;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_KEYTAB;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link SecurityConfiguration}. */
class SecurityConfigurationTest {

    @Test
    public void keytabWithoutPrincipalShouldThrowException() {
        Configuration configuration = new Configuration();
        configuration.set(KERBEROS_LOGIN_KEYTAB, "keytab.file");

        IllegalConfigurationException e =
                assertThrows(
                        IllegalConfigurationException.class,
                        () -> new SecurityConfiguration(configuration));
        assertTrue(
                e.getMessage()
                        .contains("either both keytab and principal must be defined, or neither"));
    }

    @Test
    public void principalWithoutKeytabShouldThrowException() {
        Configuration configuration = new Configuration();
        configuration.set(KERBEROS_LOGIN_PRINCIPAL, "principal");

        IllegalConfigurationException e =
                assertThrows(
                        IllegalConfigurationException.class,
                        () -> new SecurityConfiguration(configuration));
        assertTrue(
                e.getMessage()
                        .contains("either both keytab and principal must be defined, or neither"));
    }

    @Test
    public void notExistingKeytabShouldThrowException() {
        Configuration configuration = new Configuration();
        configuration.set(KERBEROS_LOGIN_KEYTAB, "nonexistingkeytab.file");
        configuration.set(KERBEROS_LOGIN_PRINCIPAL, "principal");

        IllegalConfigurationException e =
                assertThrows(
                        IllegalConfigurationException.class,
                        () -> new SecurityConfiguration(configuration));
        assertTrue(e.getMessage().contains("nonexistingkeytab.file"));
        assertTrue(e.getMessage().contains("doesn't exist"));
    }

    @Test
    public void securityRelevantConfigChangesShouldAffectEquality() {
        final String[] securityRelevantKeys = {
            "security.test.option",
            "zookeeper.sasl.test.option",
            "flink.hadoop.test.option",
            "fs.hdfs.test.option",
            CoreOptions.TMP_DIRS.key()
        };

        for (String key : securityRelevantKeys) {
            final Configuration baseConfiguration = new Configuration();
            final Configuration changedConfiguration = new Configuration();
            changedConfiguration.setString(key, "value");

            assertNotEquals(
                    new SecurityConfiguration(baseConfiguration),
                    new SecurityConfiguration(changedConfiguration),
                    key);
        }
    }

    @Test
    public void unrelatedConfigChangesShouldNotAffectEquality() {
        final Configuration baseConfiguration = new Configuration();
        final Configuration changedConfiguration = new Configuration();
        changedConfiguration.setString(PipelineOptions.JARS.key(), "file:///tmp/job.jar");
        changedConfiguration.setString(DeploymentOptions.TARGET.key(), "embedded");

        assertEquals(
                new SecurityConfiguration(baseConfiguration),
                new SecurityConfiguration(changedConfiguration));
    }
}
