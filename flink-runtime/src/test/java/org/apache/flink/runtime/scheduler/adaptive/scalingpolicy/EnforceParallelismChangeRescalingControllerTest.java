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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RescalingController}. */
class EnforceParallelismChangeRescalingControllerTest {

    private static final JobVertexID jobVertexId = new JobVertexID();

    @Test
    void testScaleUp() {
        final RescalingController rescalingController =
                new EnforceParallelismChangeRescalingController();
        assertThat(rescalingController.shouldRescale(forParallelism(1), forParallelism(2)))
                .isTrue();
    }

    @Test
    void testAlwaysScaleDown() {
        final RescalingController rescalingController =
                new EnforceParallelismChangeRescalingController();
        assertThat(rescalingController.shouldRescale(forParallelism(2), forParallelism(1)))
                .isTrue();
    }

    @Test
    void testNoScaleOnSameParallelism() {
        final RescalingController rescalingController =
                new EnforceParallelismChangeRescalingController();
        assertThat(rescalingController.shouldRescale(forParallelism(2), forParallelism(2)))
                .isFalse();
    }

    private static VertexParallelism forParallelism(int parallelism) {
        return new VertexParallelism(Collections.singletonMap(jobVertexId, parallelism));
    }
}
