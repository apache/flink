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

package org.apache.flink.tools.ci.utils.notice;

import org.apache.flink.tools.ci.utils.shared.Dependency;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NoticeParserTest {
    @Test
    void testParseNoticeFileCommonPath() {
        final String module = "some-module";
        final Dependency dependency1 = Dependency.create("groupId1", "artifactId1", "version1");
        final Dependency dependency2 = Dependency.create("groupId2", "artifactId2", "version2");
        final List<String> noticeContents =
                Arrays.asList(
                        module,
                        "",
                        "Some text about the applicable license",
                        "- " + dependency1,
                        "- " + dependency2,
                        "",
                        "some epilogue");

        assertThat(NoticeParser.parseNoticeFile(noticeContents))
                .hasValueSatisfying(
                        contents -> {
                            assertThat(contents.getNoticeModuleName()).isEqualTo(module);
                            assertThat(contents.getDeclaredDependencies())
                                    .containsExactlyInAnyOrder(dependency1, dependency2);
                        });
    }

    @Test
    void testParseNoticeFileBundlesPath() {
        final String module = "some-module";
        final Dependency dependency = Dependency.create("groupId", "artifactId", "version");
        final List<String> noticeContents =
                Arrays.asList(module, "", "Something bundles \"" + dependency + "\"");

        assertThat(NoticeParser.parseNoticeFile(noticeContents))
                .hasValueSatisfying(
                        contents -> {
                            assertThat(contents.getNoticeModuleName()).isEqualTo(module);
                            assertThat(contents.getDeclaredDependencies())
                                    .containsExactlyInAnyOrder(dependency);
                        });
    }

    @Test
    void testParseNoticeFileMalformedDependencyIgnored() {
        final String module = "some-module";
        final Dependency dependency = Dependency.create("groupId", "artifactId", "version");
        final List<String> noticeContents = Arrays.asList(module, "- " + dependency, "- a:b");

        assertThat(NoticeParser.parseNoticeFile(noticeContents))
                .hasValueSatisfying(
                        contents -> {
                            assertThat(contents.getNoticeModuleName()).isEqualTo(module);
                            assertThat(contents.getDeclaredDependencies())
                                    .containsExactlyInAnyOrder(dependency);
                        });
    }
}
