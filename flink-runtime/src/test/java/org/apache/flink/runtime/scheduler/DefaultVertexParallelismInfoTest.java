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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultVertexParallelismInfo}. */
class DefaultVertexParallelismInfoTest {
    private static final Function<Integer, Optional<String>> ALWAYS_VALID =
            (max) -> Optional.empty();

    @Test
    void parallelismInvalid() {
        assertThatThrownBy(() -> new DefaultVertexParallelismInfo(-2, 1, ALWAYS_VALID))
                .withFailMessage("parallelism is not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parallelismValid() {
        new DefaultVertexParallelismInfo(10, 1, ALWAYS_VALID);
        new DefaultVertexParallelismInfo(ExecutionConfig.PARALLELISM_DEFAULT, 1, ALWAYS_VALID);
    }

    @Test
    void maxParallelismInvalid() {
        assertThatThrownBy(() -> new DefaultVertexParallelismInfo(1, -1, ALWAYS_VALID))
                .withFailMessage("max parallelism is not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetParallelism() {
        DefaultVertexParallelismInfo info =
                new DefaultVertexParallelismInfo(
                        ExecutionConfig.PARALLELISM_DEFAULT, 10, ALWAYS_VALID);

        // test set negative value
        assertThatThrownBy(() -> info.setParallelism(-1))
                .withFailMessage("parallelism is not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);

        // test parallelism larger than max parallelism
        assertThatThrownBy(() -> info.setParallelism(11))
                .withFailMessage(
                        "Vertex's parallelism should be smaller than or equal to vertex's max parallelism.")
                .isInstanceOf(IllegalArgumentException.class);

        // set valid value.
        info.setParallelism(5);

        // test set parallelism for vertex whose parallelism was decided.
        assertThatThrownBy(() -> info.setParallelism(5))
                .withFailMessage(
                        "Vertex's parallelism can be set only if the vertex's parallelism was not decided yet.")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void setAutoMax() {
        DefaultVertexParallelismInfo info =
                new DefaultVertexParallelismInfo(
                        1, ExecutionConfig.PARALLELISM_AUTO_MAX, ALWAYS_VALID);

        assertThat(info.getMaxParallelism())
                .isEqualTo(KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM);
    }

    @Test
    void canRescaleMaxOutOfBounds() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThatThrownBy(() -> info.canRescaleMaxParallelism(-4))
                .withFailMessage("not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canRescaleMaxAuto() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThat(info.canRescaleMaxParallelism(ExecutionConfig.PARALLELISM_AUTO_MAX)).isTrue();
    }

    @Test
    void canRescaleMax() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThat(info.canRescaleMaxParallelism(3)).isTrue();
    }

    @Test
    void canRescaleMaxDefault() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThat(info.canRescaleMaxParallelism(JobVertex.MAX_PARALLELISM_DEFAULT)).isFalse();
    }

    @Test
    void setMaxOutOfBounds() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThatThrownBy(() -> info.setMaxParallelism(-4))
                .withFailMessage("not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void setMaxInvalid() {
        DefaultVertexParallelismInfo info =
                new DefaultVertexParallelismInfo(1, 1, (max) -> Optional.of("not valid"));

        assertThatThrownBy(() -> info.setMaxParallelism(4))
                .withFailMessage("not valid")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void setMaxValid() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        info.setMaxParallelism(40);

        assertThat(info.getMaxParallelism()).isEqualTo(40);
    }
}
