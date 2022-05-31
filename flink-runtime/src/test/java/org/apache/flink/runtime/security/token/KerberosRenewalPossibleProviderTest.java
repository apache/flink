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
import org.apache.flink.runtime.security.SecurityConfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_KEYTAB;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_PRINCIPAL;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KerberosRenewalPossibleProviderTest {
    @Test
    public void isRenewalPossibleMustGiveBackFalseByDefault() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setBoolean(KERBEROS_LOGIN_USETICKETCACHE, false);
        SecurityConfiguration securityConfiguration = new SecurityConfiguration(configuration);
        KerberosRenewalPossibleProvider kerberosRenewalPossibleProvider =
                new KerberosRenewalPossibleProvider(securityConfiguration);

        assertFalse(kerberosRenewalPossibleProvider.isRenewalPossible());
    }

    @Test
    public void isRenewalPossibleMustGiveBackTrueWhenKeytab(@TempDir Path tmpDir)
            throws IOException {
        Configuration configuration = new Configuration();
        configuration.setString(KERBEROS_LOGIN_PRINCIPAL, "principal");
        final Path keyTab = Files.createFile(tmpDir.resolve("test.keytab"));
        configuration.setString(KERBEROS_LOGIN_KEYTAB, keyTab.toAbsolutePath().toString());
        SecurityConfiguration securityConfiguration = new SecurityConfiguration(configuration);
        KerberosRenewalPossibleProvider kerberosRenewalPossibleProvider =
                new KerberosRenewalPossibleProvider(securityConfiguration);

        assertTrue(kerberosRenewalPossibleProvider.isRenewalPossible());
    }

    @Test
    public void isRenewalPossibleMustGiveBackTrueWhenTGT() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setBoolean(KERBEROS_LOGIN_USETICKETCACHE, true);
        SecurityConfiguration securityConfiguration = new SecurityConfiguration(configuration);
        KerberosRenewalPossibleProvider kerberosRenewalPossibleProvider =
                new KerberosRenewalPossibleProvider(securityConfiguration) {
                    @Override
                    protected boolean hasCurrentUserCredentials() {
                        return true;
                    }
                };

        assertTrue(kerberosRenewalPossibleProvider.isRenewalPossible());
    }
}
