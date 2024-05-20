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

package org.apache.flink.table.tpch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class TpchResultComparatorTest {

    @ParameterizedTest
    @CsvSource({ //
        "true, true", //
        "false, false", //
        "A, A", //
        "A, \"A\"", //
        "abc , abc", //
        "abc, abc  ", //
        "-5, \"-5\"", //
        "12, 12", //
        "12, 12", //
        "12, 12.0", //
        "13.05, 13.04", //
        "13.05, 13.14", //
        "14.05, 14.15", //
    })
    void testValidateColumnEqual(final String expected, final String actual) {
        assertThat(TpchResultComparator.validateColumn(expected, actual)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({ //
        "true, false", //
        "false, true", //
        "\"A\", A", // expected column doesn't expect quoted values
        "Abc, abc", // case sensitive
        "3, 2", //
        "-3, 3", //
        "17, \"18\"", //
        "14.05, 14.50", //
    })
    void testValidateColumnNotEqual(final String expected, final String actual) {
        assertThat(TpchResultComparator.validateColumn(expected, actual)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({ //
        "532348211.65, 5.323482116499983E8", //
        "505822441.4861, 5.05822441486102E8", //
        "526165934.000839, 5.261659340008392E8", //
        "0.05, 0.05008133906963965", //
    })
    void testValidateColumnDoubles(final String expected, final String actual) {
        assertThat(TpchResultComparator.validateColumn(expected, actual)).isTrue();
    }
}
