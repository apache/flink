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

package org.apache.flink.runtime.scheduler.adaptive.scalingpolicy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RescalingController}. */
@ExtendWith(TestLoggerExtension.class)
public class EnforceMinimalIncreaseRescalingControllerTest {
    private static final Configuration TEST_CONFIG =
            new Configuration().set(JobManagerOptions.MIN_PARALLELISM_INCREASE, 2);

    private static final JobVertexID jobVertexId = new JobVertexID();

    @Test
    void testScaleUp() {
        final RescalingController rescalingController =
                new EnforceMinimalIncreaseRescalingController(TEST_CONFIG);
        assertThat(rescalingController.shouldRescale(forParallelism(1), forParallelism(4)))
                .isTrue();
    }

    @Test
    void testNoScaleUp() {
        final RescalingController rescalingController =
                new EnforceMinimalIncreaseRescalingController(TEST_CONFIG);
        assertThat(rescalingController.shouldRescale(forParallelism(2), forParallelism(3)))
                .isFalse();
    }

    @Test
    void testAlwaysScaleDown() {
        final RescalingController rescalingController =
                new EnforceMinimalIncreaseRescalingController(TEST_CONFIG);
        assertThat(rescalingController.shouldRescale(forParallelism(2), forParallelism(1)))
                .isTrue();
    }

    @Test
    void testNoScaleOnSameParallelism() {
        final RescalingController rescalingController =
                new EnforceMinimalIncreaseRescalingController(TEST_CONFIG);
        assertThat(rescalingController.shouldRescale(forParallelism(2), forParallelism(2)))
                .isFalse();
    }

    private static VertexParallelism forParallelism(int parallelism) {
        return new VertexParallelism(Collections.singletonMap(jobVertexId, parallelism));
    }
}
