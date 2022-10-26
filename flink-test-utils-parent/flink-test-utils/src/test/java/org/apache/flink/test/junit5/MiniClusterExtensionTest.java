/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.junit5;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MiniClusterExtension.class)
class MiniClusterExtensionTest {

    @Test
    void testInjectClusterClient(@InjectClusterClient ClusterClient<?> clusterClient) {
        assertThat(clusterClient).isNotNull();
    }

    @Test
    void testInjectRestClusterClient(@InjectClusterClient RestClusterClient<?> clusterClient) {
        assertThat(clusterClient).isNotNull();
    }

    @Test
    void testInjectMiniCluster(@InjectMiniCluster MiniCluster miniCluster) {
        assertThat(miniCluster).isNotNull();
    }

    @Test
    void testInjectClusterRESTAddress(@InjectClusterRESTAddress URI address) {
        assertThat(address).isNotNull();
    }

    @Test
    void testInjectClusterClientConfiguration(
            @InjectClusterClientConfiguration Configuration config) {
        assertThat(config).isNotNull();
    }
}
