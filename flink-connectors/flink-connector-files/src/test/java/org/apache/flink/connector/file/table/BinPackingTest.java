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

package org.apache.flink.connector.file.table;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinPacking}. */
class BinPackingTest {

    @Test
    void testBinPacking() {
        assertThat(asList(asList(1, 2), singletonList(3), singletonList(4), singletonList(5)))
                .as("Should pack the first 2 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 3));

        assertThat(asList(asList(1, 2), singletonList(3), singletonList(4), singletonList(5)))
                .as("Should pack the first 2 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 5));

        assertThat(asList(asList(1, 2, 3), singletonList(4), singletonList(5)))
                .as("Should pack the first 3 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 6));

        assertThat(asList(asList(1, 2, 3), singletonList(4), singletonList(5)))
                .as("Should pack the first 3 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 8));

        assertThat(asList(asList(1, 2, 3), asList(4, 5)))
                .as("Should pack the first 3 values, last 2 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 9));

        assertThat(asList(asList(1, 2, 3, 4), singletonList(5)))
                .as("Should pack the first 4 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 10));

        assertThat(asList(asList(1, 2, 3, 4), singletonList(5)))
                .as("Should pack the first 4 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 14));

        assertThat(singletonList(asList(1, 2, 3, 4, 5)))
                .as("Should pack the first 5 values")
                .isEqualTo(pack(asList(1, 2, 3, 4, 5), 15));
    }

    private List<List<Integer>> pack(List<Integer> items, long targetWeight) {
        return BinPacking.pack(items, Integer::longValue, targetWeight);
    }
}
