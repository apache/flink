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

package org.apache.flink.runtime.taskprocessing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link KeyAccountingUnit}. */
class keyAccountingUnitTest {

    @Test
    void testBasic() {
        KeyAccountingUnit<String, Integer> keyAccountingUnit = new KeyAccountingUnit<>();
        assertThat(keyAccountingUnit.available("record1", 1)).isTrue();
        keyAccountingUnit.occupy("record1", 1);
        assertThat(keyAccountingUnit.available("record1", 1)).isTrue();
        assertThat(keyAccountingUnit.available("record2", 2)).isTrue();
        assertThat(keyAccountingUnit.available("record3", 1)).isFalse();
        assertThatThrownBy(() -> keyAccountingUnit.occupy("record3", 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The record record3(1) is already occupied.");
        keyAccountingUnit.release("record1", 1);
        assertThat(keyAccountingUnit.available("record2", 1)).isTrue();
    }
}
