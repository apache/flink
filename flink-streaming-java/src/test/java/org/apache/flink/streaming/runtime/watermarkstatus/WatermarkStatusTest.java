/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link WatermarkStatus}. */
class WatermarkStatusTest {

    @Test
    void testIllegalCreationThrowsException() {
        assertThatThrownBy(() -> new WatermarkStatus(32))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEquals() {
        WatermarkStatus idleStatus = new WatermarkStatus(WatermarkStatus.IDLE_STATUS);
        WatermarkStatus activeStatus = new WatermarkStatus(WatermarkStatus.ACTIVE_STATUS);

        assertThat(idleStatus).isEqualTo(WatermarkStatus.IDLE);
        assertThat(idleStatus.isIdle()).isTrue();
        assertThat(idleStatus.isActive()).isFalse();

        assertThat(activeStatus).isEqualTo(WatermarkStatus.ACTIVE);
        assertThat(activeStatus.isActive()).isTrue();
        assertThat(activeStatus.isIdle()).isFalse();
    }

    @Test
    void testTypeCasting() {
        WatermarkStatus status = WatermarkStatus.ACTIVE;

        assertThat(status.isWatermarkStatus()).isTrue();
        assertThat(status.isRecord()).isFalse();
        assertThat(status.isWatermark()).isFalse();
        assertThat(status.isLatencyMarker()).isFalse();

        assertThatThrownBy(status::asWatermark).isInstanceOf(ClassCastException.class);

        assertThatThrownBy(status::asRecord).isInstanceOf(ClassCastException.class);

        assertThatThrownBy(status::asLatencyMarker).isInstanceOf(ClassCastException.class);
    }
}
