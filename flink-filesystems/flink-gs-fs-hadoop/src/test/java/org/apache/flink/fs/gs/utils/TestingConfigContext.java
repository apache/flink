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

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Optional;

/** Implementation of ConfigUtils.ConfigContext for testing. */
public class TestingConfigContext implements ConfigUtils.ConfigContext {

    private final Map<String, String> envs;
    private final Map<String, org.apache.hadoop.conf.Configuration> hadoopConfigs;
    private final Map<String, GoogleCredentials> credentials;

    public TestingConfigContext(
            Map<String, String> envs,
            Map<String, org.apache.hadoop.conf.Configuration> hadoopConfigs,
            Map<String, GoogleCredentials> credentials) {
        this.envs = envs;
        this.hadoopConfigs = hadoopConfigs;
        this.credentials = credentials;
    }

    @Override
    public Optional<String> getenv(String name) {
        return Optional.ofNullable(envs.get(name));
    }

    @Override
    public Configuration loadHadoopConfigFromDir(String configDir) {
        return Optional.ofNullable(hadoopConfigs.get(configDir))
                .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public GoogleCredentials loadStorageCredentialsFromFile(String credentialsPath) {
        return Optional.ofNullable(credentials.get(credentialsPath))
                .orElseThrow(IllegalArgumentException::new);
    }
}
