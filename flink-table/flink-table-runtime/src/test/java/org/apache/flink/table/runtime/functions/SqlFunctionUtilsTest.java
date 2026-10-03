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

package org.apache.flink.table.runtime.functions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlFunctionUtils}. */
class SqlFunctionUtilsTest {

    @Test
    void testParseUrlQueryParameterWithDifferentKeys() {
        String url = "http://flink.apache.org/a?k1=v1&k2=v2&k.3=v3&k434=v4";

        assertThat(SqlFunctionUtils.parseUrl(url, "QUERY", "k1")).isEqualTo("v1");
        assertThat(SqlFunctionUtils.parseUrl(url, "QUERY", "k2")).isEqualTo("v2");
        assertThat(SqlFunctionUtils.parseUrl(url, "QUERY", "k.3")).isEqualTo("v3");
        assertThat(SqlFunctionUtils.parseUrl(url, "QUERY", "k4.4")).isNull();
        assertThat(SqlFunctionUtils.parseUrl(url, "QUERY", "missing")).isNull();
        assertThat(SqlFunctionUtils.parseUrl(url, "QUERY", "k1")).isEqualTo("v1");
    }
}
