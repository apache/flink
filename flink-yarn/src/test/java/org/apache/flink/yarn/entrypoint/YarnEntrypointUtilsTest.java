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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.yarn.YarnConfigKeys;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link YarnEntrypointUtils}. */
class YarnEntrypointUtilsTest {

    @TempDir private static Path tempBaseDir;

    /**
     * Tests that the REST ports are correctly set when loading a {@link Configuration} with
     * unspecified REST options.
     */
    @Test
    void testRestPortOptionsUnspecified() throws IOException {
        final Configuration initialConfiguration = new Configuration();

        final Configuration configuration = loadConfiguration(initialConfiguration);

        // having not specified the ports should set the rest bind port to 0
        assertThat(configuration.get(RestOptions.BIND_PORT)).isEqualTo("0");
    }

    /** Tests that the binding REST port is set to the REST port if set. */
    @Test
    void testRestPortSpecified() throws IOException {
        final Configuration initialConfiguration = new Configuration();
        final int port = 1337;
        initialConfiguration.set(RestOptions.PORT, port);

        final Configuration configuration = loadConfiguration(initialConfiguration);

        // if the bind port is not specified it should fall back to the rest port
        assertThat(configuration.get(RestOptions.BIND_PORT)).isEqualTo(String.valueOf(port));
    }

    /** Tests that the binding REST port has precedence over the REST port if both are set. */
    @Test
    void testRestPortAndBindingPortSpecified() throws IOException {
        final Configuration initialConfiguration = new Configuration();
        final int port = 1337;
        final String bindingPortRange = "1337-7331";
        initialConfiguration.set(RestOptions.PORT, port);
        initialConfiguration.set(RestOptions.BIND_PORT, bindingPortRange);

        final Configuration configuration = loadConfiguration(initialConfiguration);

        // bind port should have precedence over the rest port
        assertThat(configuration.get(RestOptions.BIND_PORT)).isEqualTo(bindingPortRange);
    }

    @Test
    void testRestBindingAddressUnspecified() throws IOException {
        final Configuration initialConfiguration = new Configuration();
        final Configuration configuration = loadConfiguration(initialConfiguration);

        assertThat(configuration.get(RestOptions.BIND_ADDRESS)).isEqualTo("foobar");
    }

    @Test
    void testRestBindingAddressSpecified() throws Exception {
        final Configuration initialConfiguration = new Configuration();
        final String bindingAddress = "0.0.0.0";
        initialConfiguration.set(RestOptions.BIND_ADDRESS, bindingAddress);
        final Configuration configuration = loadConfiguration(initialConfiguration);

        assertThat(configuration.get(RestOptions.BIND_ADDRESS)).isEqualTo(bindingAddress);
    }

    @Test
    void testParsingValidKerberosEnv() throws IOException {
        final Configuration initialConfiguration = new Configuration();
        Map<String, String> env = new HashMap<>();
        File keytabFile =
                Files.createTempFile(tempBaseDir, UUID.randomUUID().toString(), "").toFile();
        env.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, keytabFile.getAbsolutePath());
        env.put(YarnConfigKeys.KEYTAB_PRINCIPAL, "starlord");

        Configuration configuration = loadConfiguration(initialConfiguration, env);

        assertThat(configuration.get(SecurityOptions.KERBEROS_LOGIN_KEYTAB))
                .isEqualTo(keytabFile.getAbsolutePath());
        assertThat(configuration.get(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL))
                .isEqualTo("starlord");
    }

    @Test
    void testParsingKerberosEnvWithMissingKeytab() throws IOException {
        final Configuration initialConfiguration = new Configuration();
        Map<String, String> env = new HashMap<>();
        env.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, "/hopefully/doesnt/exist");
        env.put(YarnConfigKeys.KEYTAB_PRINCIPAL, "starlord");

        Configuration configuration = loadConfiguration(initialConfiguration, env);

        // both keytab and principal should be null
        assertThat(configuration.get(SecurityOptions.KERBEROS_LOGIN_KEYTAB)).isNull();
        assertThat(configuration.get(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL)).isNull();
    }

    @Test
    void testDynamicParameterOverloading() throws IOException {
        final Configuration initialConfiguration = new Configuration();
        initialConfiguration.set(JobManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(1));

        Configuration dynamicParameters = new Configuration();
        dynamicParameters.set(JobManagerOptions.JVM_METASPACE, MemorySize.MAX_VALUE);
        Configuration overloadedConfiguration =
                loadConfiguration(initialConfiguration, dynamicParameters);

        assertThat(overloadedConfiguration.get(JobManagerOptions.JVM_METASPACE))
                .isEqualTo(MemorySize.MAX_VALUE);
    }

    @Nonnull
    private static Configuration loadConfiguration(Configuration initialConfiguration)
            throws IOException {
        return loadConfiguration(initialConfiguration, new HashMap<>());
    }

    @Nonnull
    private static Configuration loadConfiguration(
            Configuration initialConfiguration, Configuration dynamicParameters)
            throws IOException {
        return loadConfiguration(initialConfiguration, dynamicParameters, new HashMap<>());
    }

    @Nonnull
    private static Configuration loadConfiguration(
            Configuration initialConfiguration, Map<String, String> env) throws IOException {
        return loadConfiguration(initialConfiguration, new Configuration(), env);
    }

    @Nonnull
    private static Configuration loadConfiguration(
            Configuration initialConfiguration,
            Configuration dynamicParameters,
            Map<String, String> env)
            throws IOException {
        final File workingDirectory =
                Files.createTempDirectory(tempBaseDir, UUID.randomUUID().toString()).toFile();
        env.put(ApplicationConstants.Environment.NM_HOST.key(), "foobar");
        BootstrapTools.writeConfiguration(
                initialConfiguration, new File(workingDirectory, "config.yaml"));
        return YarnEntrypointUtils.loadConfiguration(
                workingDirectory.getAbsolutePath(), dynamicParameters, env);
    }
}
