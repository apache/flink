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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** unit test for ClientOptions. */
class ClientOptionsTest {

    @Test
    void testGetClientTimeout() {
        Configuration configuration = new Configuration();
        configuration.set(ClientOptions.CLIENT_TIMEOUT, Duration.ofSeconds(10));

        assertThat(configuration.get(ClientOptions.CLIENT_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(10));

        configuration = new Configuration();
        configuration.set(ClientOptions.CLIENT_TIMEOUT, Duration.ofSeconds(20));
        assertThat(configuration.get(ClientOptions.CLIENT_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(20));

        configuration = new Configuration();
        assertThat(configuration.get(ClientOptions.CLIENT_TIMEOUT))
                .isEqualTo(ClientOptions.CLIENT_TIMEOUT.defaultValue());
    }
}
