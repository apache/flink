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

package org.apache.flink.runtime.webmonitor.history;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ArchivedJson}. */
class ArchivedJsonTest {

    @Test
    void testEquals() {
        ArchivedJson original = new ArchivedJson("path", "json");
        ArchivedJson twin = new ArchivedJson("path", "json");
        ArchivedJson identicalPath = new ArchivedJson("path", "hello");
        ArchivedJson identicalJson = new ArchivedJson("hello", "json");

        assertThat(original).isEqualTo(original);
        assertThat(twin).isEqualTo(original);
        assertThat(identicalPath).isNotEqualTo(original);
        assertThat(identicalJson).isNotEqualTo(original);
    }

    @Test
    void testHashCode() {
        ArchivedJson original = new ArchivedJson("path", "json");
        ArchivedJson twin = new ArchivedJson("path", "json");

        assertThat(original).isEqualTo(original);
        assertThat(twin).isEqualTo(original);
        assertThat(twin).hasSameHashCodeAs(original);
    }
}
