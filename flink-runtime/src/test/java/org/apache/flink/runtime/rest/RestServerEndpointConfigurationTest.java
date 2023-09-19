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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.util.ConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RestServerEndpointConfiguration}. */
class RestServerEndpointConfigurationTest {

    private static final String ADDRESS = "123.123.123.123";
    private static final String BIND_ADDRESS = "023.023.023.023";
    private static final String BIND_PORT = "7282";
    private static final int CONTENT_LENGTH = 1234;

    @Test
    void testBasicMapping(@TempDir File file) throws ConfigurationException {
        Configuration originalConfig = new Configuration();
        originalConfig.setString(RestOptions.ADDRESS, ADDRESS);
        originalConfig.setString(RestOptions.BIND_ADDRESS, BIND_ADDRESS);
        originalConfig.setString(RestOptions.BIND_PORT, BIND_PORT);
        originalConfig.setInteger(RestOptions.SERVER_MAX_CONTENT_LENGTH, CONTENT_LENGTH);
        originalConfig.setString(WebOptions.TMP_DIR, file.getAbsolutePath());

        final RestServerEndpointConfiguration result =
                RestServerEndpointConfiguration.fromConfiguration(originalConfig);
        assertThat(result.getRestAddress()).isEqualTo(ADDRESS);
        assertThat(result.getRestBindAddress()).isEqualTo(BIND_ADDRESS);
        assertThat(result.getRestBindPortRange()).isEqualTo(BIND_PORT);
        assertThat(result.getMaxContentLength()).isEqualTo(CONTENT_LENGTH);
        assertThat(result.getUploadDir().toAbsolutePath().toString())
                .contains(file.getAbsolutePath());
    }
}
