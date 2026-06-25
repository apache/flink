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

package org.apache.flink.table.runtime.functions.scalar;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Smoke tests for {@link GeographyWkbBenchmark}. */
class GeographyWkbBenchmarkTest {

    @Test
    void testBenchmarksRunForRepresentativePayloads() throws IOException {
        for (GeographyWkbBenchmark.PayloadKind payloadKind :
                GeographyWkbBenchmark.PayloadKind.values()) {
            final GeographyWkbBenchmark benchmark = new GeographyWkbBenchmark();
            benchmark.setup(payloadKind);

            final Map<String, GeographyWkbBenchmark.BenchmarkResult> results =
                    benchmark.runAllBenchmarks(128);

            assertThat(benchmark.getPayloadKind()).isEqualTo(payloadKind);
            assertThat(results).hasSize(7);
            assertThat(results.values())
                    .allSatisfy(
                            result -> {
                                assertThat(result.getNanosPerOperation())
                                        .isGreaterThanOrEqualTo(0L);
                                assertThat(result.getChecksum()).isGreaterThan(0L);
                            });
        }
    }
}
