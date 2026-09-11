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

package org.apache.flink.fs.cse;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CseOptions}. */
class CseOptionsTest {

    @ParameterizedTest
    @MethodSource("encryptionContextCases")
    void shouldExtractEncryptionContext(
            final Map<String, String> configEntries, final Map<String, String> expected) {
        final Configuration config = new Configuration();
        configEntries.forEach(config::setString);

        assertThat(CseOptions.extractEncryptionContext(config))
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    static Stream<Arguments> encryptionContextCases() {
        return Stream.of(
                // multiple context entries
                Arguments.of(
                        Map.of(
                                "fs.cse.encryption-context.org_id", "org-123",
                                "fs.cse.encryption-context.pool_id", "lfcp-456"),
                        Map.of("org_id", "org-123", "pool_id", "lfcp-456")),
                // no context entries — only unrelated keys
                Arguments.of(Map.of("fs.cse.write-key-id", "some-key"), Map.of()),
                // empty config
                Arguments.of(Map.of(), Map.of()),
                // mixed: context entries alongside unrelated keys
                Arguments.of(
                        Map.of(
                                "fs.cse.encryption-context.org_id", "org-123",
                                "fs.cse.write-key-id", "some-key",
                                "fs.azure.account-key", "secret"),
                        Map.of("org_id", "org-123")));
    }
}
