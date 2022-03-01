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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link AkkaRpcSystemLoader}.
 *
 * <p>This must be an ITCase so that it runs after the 'package' phase of maven. Otherwise the
 * flink-rpc-akka jar will not be available.
 */
public class AkkaRpcSystemLoaderITCase extends TestLogger {

    private static final AkkaRpcSystemLoader LOADER = new AkkaRpcSystemLoader();

    @ClassRule public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

    @Test
    public void testServiceLoadingWithDefaultConfig() {
        final Configuration config = new Configuration();
        try (final RpcSystem rpcSystem = LOADER.loadRpcSystem(config)) {
            assertThat(rpcSystem, not(nullValue()));
        }
    }

    @Test
    public void testServiceLoadingWithNonExistingPath() {
        final Configuration config = new Configuration();
        config.set(
                CoreOptions.TMP_DIRS,
                TMP_DIR.getRoot().toPath().resolve(Paths.get("some", "directory")).toString());
        try (final RpcSystem rpcSystem = LOADER.loadRpcSystem(config)) {
            assertThat(rpcSystem, not(nullValue()));
        }
    }
}
