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

package org.apache.flink.runtime.dispatcher.benchmark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/** Test for {@link DispatcherHighAvailabilityServiceBenchmark}. */
class DispatcherHighAvailabilityServiceBenchmarkTest {

    @Test
    void testDispatcherZKHaService(@TempDir Path path) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());

        DispatcherHighAvailabilityServiceBenchmark benchmark =
                new DispatcherHighAvailabilityServiceBenchmark();
        benchmark.setup(configuration, path.toFile());
        benchmark.submitJob();
        benchmark.close();
    }

    @Test
    void testDispatcherNoneHaService(@TempDir Path path) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.NONE.name());

        DispatcherHighAvailabilityServiceBenchmark benchmark =
                new DispatcherHighAvailabilityServiceBenchmark();
        benchmark.setup(configuration, path.toFile());
        benchmark.submitJob();
        benchmark.close();
    }
}
