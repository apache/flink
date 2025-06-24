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

package org.apache.flink.runtime.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResourceManagerUtilsTest {

    @Test
    void testParseRestBindPortFromWebInterfaceUrlWithEmptyUrl() {
        assertThat(ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl("")).isEqualTo(-1);
    }

    @Test
    void testParseRestBindPortFromWebInterfaceUrlWithNullUrl() {
        assertThat(ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl(null)).isEqualTo(-1);
    }

    @Test
    void testParseRestBindPortFromWebInterfaceUrlWithInvalidSchema() {
        assertThat(ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl("localhost:8080//"))
                .isEqualTo(-1);
    }

    @Test
    void testParseRestBindPortFromWebInterfaceUrlWithInvalidPort() {
        assertThat(ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl("localhost:port1"))
                .isEqualTo(-1);
    }

    @Test
    void testParseRestBindPortFromWebInterfaceUrlWithValidPort() {
        assertThat(ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl("localhost:8080"))
                .isEqualTo(8080);
    }
}
