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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link WorkerCounter}. */
class WorkerCounterTest {

    @Test
    void testWorkerCounterIncreaseAndDecrease() {
        final WorkerResourceSpec spec1 = new WorkerResourceSpec.Builder().setCpuCores(1.0).build();
        final WorkerResourceSpec spec2 = new WorkerResourceSpec.Builder().setCpuCores(2.0).build();

        final WorkerCounter counter = new WorkerCounter();
        assertThat(counter.getTotalNum()).isZero();
        assertThat(counter.getNum(spec1)).isZero();
        assertThat(counter.getNum(spec2)).isZero();

        assertThat(counter.increaseAndGet(spec1)).isOne();
        assertThat(counter.getTotalNum()).isOne();
        assertThat(counter.getNum(spec1)).isOne();
        assertThat(counter.getNum(spec2)).isZero();

        assertThat(counter.increaseAndGet(spec1)).isEqualTo(2);
        assertThat(counter.getTotalNum()).isEqualTo(2);
        assertThat(counter.getNum(spec1)).isEqualTo(2);
        assertThat(counter.getNum(spec2)).isZero();

        assertThat(counter.increaseAndGet(spec2)).isOne();
        assertThat(counter.getTotalNum()).isEqualTo(3);
        assertThat(counter.getNum(spec1)).isEqualTo(2);
        assertThat(counter.getNum(spec2)).isOne();

        assertThat(counter.decreaseAndGet(spec1)).isOne();
        assertThat(counter.getTotalNum()).isEqualTo(2);
        assertThat(counter.getNum(spec1)).isOne();
        assertThat(counter.getNum(spec2)).isOne();

        assertThat(counter.decreaseAndGet(spec2)).isZero();
        assertThat(counter.getTotalNum()).isOne();
        assertThat(counter.getNum(spec1)).isOne();
        assertThat(counter.getNum(spec2)).isZero();
    }

    @Test
    void testWorkerCounterDecreaseOnZero() {
        final WorkerResourceSpec spec = new WorkerResourceSpec.Builder().build();
        final WorkerCounter counter = new WorkerCounter();
        assertThatThrownBy(() -> counter.decreaseAndGet(spec))
                .isInstanceOf(IllegalStateException.class);
    }
}
