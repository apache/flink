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

package org.apache.flink.fs.gs.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fs.gs.TestUtils;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the two documented credential setups give gcs-connector and the RecoverableWriter the
 * same credentials, and that Flink warns when they don't.
 */
class ConfigUtilsAuthTypeTest {

    private static final String CONF_DIR = "/hadoop/conf";

    private static final String APPLICATION_CREDENTIALS = "/opt/application.json";

    private static final String KEYFILE = "/opt/keyfile.json";

    @RegisterExtension
    private final LoggerAuditingExtension warnings =
            new LoggerAuditingExtension(ConfigUtils.class, Level.WARN);

    @Test
    void warnsForApplicationCredentialsWithoutAuthType() {
        final org.apache.hadoop.conf.Configuration hadoopConfig =
                loadCredentials(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        APPLICATION_CREDENTIALS,
                        APPLICATION_CREDENTIALS);

        assertThat(hadoopConfig.get("fs.gs.auth.type")).isNull();
        assertThat(connectorAuthType(hadoopConfig)).isEqualTo(AuthenticationType.COMPUTE_ENGINE);
        assertThat(warnings.getMessages())
                .singleElement()
                .asString()
                .contains(APPLICATION_CREDENTIALS, "uses COMPUTE_ENGINE", "to APPLICATION_DEFAULT");
    }

    @Test
    void warnsForKeyfileWithoutAuthType() {
        final org.apache.hadoop.conf.Configuration hadoopConfig =
                loadCredentials(
                        Collections.emptyMap(),
                        Collections.singletonMap(
                                "google.cloud.auth.service.account.json.keyfile", KEYFILE),
                        null,
                        KEYFILE);

        assertThat(hadoopConfig.get("fs.gs.auth.type")).isNull();
        assertThat(warnings.getMessages())
                .singleElement()
                .asString()
                .contains(KEYFILE, "to SERVICE_ACCOUNT_JSON_KEYFILE");
    }

    @Test
    void warnsForApplicationCredentialsWhenKeyfileIsAlsoSet() {
        loadCredentials(
                Collections.emptyMap(),
                Collections.singletonMap("google.cloud.auth.service.account.json.keyfile", KEYFILE),
                APPLICATION_CREDENTIALS,
                APPLICATION_CREDENTIALS);

        assertThat(warnings.getMessages())
                .singleElement()
                .asString()
                .contains(APPLICATION_CREDENTIALS, "to APPLICATION_DEFAULT")
                .doesNotContain(KEYFILE);
    }

    @Test
    void applicationDefaultSetupUsesApplicationCredentials() {
        final org.apache.hadoop.conf.Configuration hadoopConfig =
                loadCredentials(
                        Collections.singletonMap("gs.auth.type", "APPLICATION_DEFAULT"),
                        Collections.emptyMap(),
                        APPLICATION_CREDENTIALS,
                        APPLICATION_CREDENTIALS);

        assertThat(connectorAuthType(hadoopConfig))
                .isEqualTo(AuthenticationType.APPLICATION_DEFAULT);
        assertThat(warnings.getMessages()).isEmpty();
    }

    @Test
    void keyfileSetupUsesKeyfile() {
        final org.apache.hadoop.conf.Configuration hadoopConfig =
                loadCredentials(
                        Collections.singletonMap("gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE"),
                        Collections.singletonMap(
                                "google.cloud.auth.service.account.json.keyfile", KEYFILE),
                        null,
                        KEYFILE);

        assertThat(connectorAuthType(hadoopConfig))
                .isEqualTo(AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
        assertThat(
                        HadoopCredentialsConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX
                                .withPrefixes(
                                        GoogleHadoopFileSystemConfiguration.CONFIG_KEY_PREFIXES)
                                .get(hadoopConfig, hadoopConfig::get))
                .isEqualTo(KEYFILE);
        assertThat(warnings.getMessages()).isEmpty();
    }

    @Test
    void doesNotWarnForAuthTypeWithHadoopPrefix() {
        final org.apache.hadoop.conf.Configuration hadoopConfig =
                loadCredentials(
                        Collections.emptyMap(),
                        Collections.singletonMap("google.cloud.auth.type", "APPLICATION_DEFAULT"),
                        APPLICATION_CREDENTIALS,
                        APPLICATION_CREDENTIALS);

        assertThat(connectorAuthType(hadoopConfig))
                .isEqualTo(AuthenticationType.APPLICATION_DEFAULT);
        assertThat(warnings.getMessages()).isEmpty();
    }

    @Test
    void doesNotWarnWithoutCredentialsFile() {
        loadCredentials(Collections.emptyMap(), Collections.emptyMap(), null, null);

        assertThat(warnings.getMessages()).isEmpty();
    }

    @Test
    void doesNotWarnWhenServiceAccountIsDisabled() {
        loadCredentials(
                Collections.emptyMap(),
                Collections.singletonMap("google.cloud.auth.service.account.enable", "false"),
                APPLICATION_CREDENTIALS,
                null);

        assertThat(warnings.getMessages()).isEmpty();
    }

    /**
     * Builds the Hadoop config the way GSFileSystemFactory does, loads the RecoverableWriter
     * credentials and checks which file they came from.
     */
    private static org.apache.hadoop.conf.Configuration loadCredentials(
            Map<String, String> flinkOptions,
            Map<String, String> coreSite,
            String applicationCredentials,
            String expectedCredentialsFile) {
        final Map<String, String> envs = new HashMap<>();
        envs.put("HADOOP_CONF_DIR", CONF_DIR);
        if (applicationCredentials != null) {
            envs.put("GOOGLE_APPLICATION_CREDENTIALS", applicationCredentials);
        }
        final GoogleCredentials credentials = GoogleCredentials.newBuilder().build();
        final Map<String, GoogleCredentials> credentialsFiles = new HashMap<>();
        if (expectedCredentialsFile != null) {
            credentialsFiles.put(expectedCredentialsFile, credentials);
        }
        final TestingConfigContext configContext =
                new TestingConfigContext(
                        envs,
                        Collections.singletonMap(CONF_DIR, TestUtils.hadoopConfigFromMap(coreSite)),
                        credentialsFiles);

        final org.apache.hadoop.conf.Configuration hadoopConfig =
                ConfigUtils.getHadoopConfiguration(
                        Configuration.fromMap(flinkOptions), configContext);
        if (expectedCredentialsFile == null) {
            assertThat(ConfigUtils.getStorageCredentials(hadoopConfig, configContext)).isEmpty();
        } else {
            assertThat(ConfigUtils.getStorageCredentials(hadoopConfig, configContext))
                    .containsSame(credentials);
        }
        return hadoopConfig;
    }

    private static AuthenticationType connectorAuthType(
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        return HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX
                .withPrefixes(GoogleHadoopFileSystemConfiguration.CONFIG_KEY_PREFIXES)
                .get(hadoopConfig, hadoopConfig::getEnum);
    }
}
