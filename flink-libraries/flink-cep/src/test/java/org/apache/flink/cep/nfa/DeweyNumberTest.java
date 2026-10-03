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

package org.apache.flink.cep.nfa;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DeweyNumber}. */
class DeweyNumberTest {

    @Test
    void testDeweyNumberGeneration() {
        DeweyNumber start = new DeweyNumber(1);
        DeweyNumber increased = start.increase();
        DeweyNumber increaseAddStage = increased.addStage();
        DeweyNumber startAddStage = start.addStage();
        DeweyNumber startAddStageIncreased = startAddStage.increase();
        DeweyNumber startAddStageIncreasedAddStage = startAddStageIncreased.addStage();

        assertThat(start).isEqualTo(DeweyNumber.fromString("1"));
        assertThat(increased).isEqualTo(DeweyNumber.fromString("2"));
        assertThat(increaseAddStage).isEqualTo(DeweyNumber.fromString("2.0"));
        assertThat(startAddStage).isEqualTo(DeweyNumber.fromString("1.0"));
        assertThat(startAddStageIncreased).isEqualTo(DeweyNumber.fromString("1.1"));
        assertThat(startAddStageIncreasedAddStage).isEqualTo(DeweyNumber.fromString("1.1.0"));

        assertThat(startAddStage.isCompatibleWith(start)).isTrue();
        assertThat(startAddStageIncreased.isCompatibleWith(startAddStage)).isTrue();
        assertThat(startAddStageIncreasedAddStage.isCompatibleWith(startAddStageIncreased))
                .isTrue();
        assertThat(startAddStageIncreasedAddStage.isCompatibleWith(startAddStage)).isFalse();
        assertThat(increaseAddStage.isCompatibleWith(startAddStage)).isFalse();
        assertThat(startAddStage.isCompatibleWith(increaseAddStage)).isFalse();
        assertThat(startAddStageIncreased.isCompatibleWith(startAddStageIncreasedAddStage))
                .isFalse();
    }

    @Test
    void testZeroSplitsDeweyNumber() {
        assertThatThrownBy(() -> DeweyNumber.fromString("."))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
