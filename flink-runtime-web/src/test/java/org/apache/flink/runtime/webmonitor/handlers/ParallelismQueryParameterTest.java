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

package org.apache.flink.runtime.webmonitor.handlers;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ParallelismQueryParameter}. */
class ParallelismQueryParameterTest {

    private final ParallelismQueryParameter parallelismQueryParameter =
            new ParallelismQueryParameter();

    @Test
    void testConvertStringToValue() {
        assertThat(parallelismQueryParameter.convertValueToString(42)).isEqualTo("42");
    }

    @Test
    void testConvertValueFromString() {
        assertThat((int) parallelismQueryParameter.convertStringToValue("42")).isEqualTo(42);
    }
}
