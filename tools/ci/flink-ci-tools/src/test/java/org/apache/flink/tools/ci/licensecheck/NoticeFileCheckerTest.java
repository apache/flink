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

package org.apache.flink.tools.ci.licensecheck;

import org.apache.flink.tools.ci.utils.notice.NoticeContents;
import org.apache.flink.tools.ci.utils.shared.Dependency;

import com.google.common.collect.ArrayListMultimap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class NoticeFileCheckerTest {

    @Test
    void testCheckNoticeFileHappyPath() {
        final String moduleName = "test";
        final Dependency bundledDependency = Dependency.create("a", "b", "c");
        final ArrayListMultimap<String, Dependency> bundleDependencies = ArrayListMultimap.create();
        bundleDependencies.put(moduleName, bundledDependency);

        assertThat(
                        NoticeFileChecker.checkNoticeFile(
                                bundleDependencies,
                                moduleName,
                                new NoticeContents(
                                        moduleName, Collections.singletonList(bundledDependency))))
                .isEmpty();
    }

    @Test
    void testCheckNoticeFileRejectsEmptyFile() {
        assertThat(NoticeFileChecker.checkNoticeFile(ArrayListMultimap.create(), "test", null))
                .containsOnlyKeys(NoticeFileChecker.Severity.CRITICAL);
    }

    @Test
    void testCheckNoticeFileToleratesModuleNameMismatch() {
        final String moduleName = "test";

        assertThat(
                        NoticeFileChecker.checkNoticeFile(
                                ArrayListMultimap.create(),
                                moduleName,
                                new NoticeContents(moduleName + "2", Collections.emptyList())))
                .containsOnlyKeys(NoticeFileChecker.Severity.TOLERATED);
    }

    @Test
    void testCheckNoticeFileRejectsDuplicateLine() {
        final String moduleName = "test";
        final ArrayListMultimap<String, Dependency> bundleDependencies = ArrayListMultimap.create();
        bundleDependencies.put(moduleName, Dependency.create("a", "b", "c"));

        assertThat(
                        NoticeFileChecker.checkNoticeFile(
                                bundleDependencies,
                                moduleName,
                                new NoticeContents(
                                        moduleName,
                                        Arrays.asList(
                                                Dependency.create("a", "b", "c"),
                                                Dependency.create("a", "b", "c")))))
                .containsOnlyKeys(NoticeFileChecker.Severity.CRITICAL);
    }

    @Test
    void testCheckNoticeFileRejectsMissingDependency() {
        final String moduleName = "test";
        final ArrayListMultimap<String, Dependency> bundleDependencies = ArrayListMultimap.create();
        bundleDependencies.put(moduleName, Dependency.create("a", "b", "c"));

        assertThat(
                        NoticeFileChecker.checkNoticeFile(
                                bundleDependencies,
                                moduleName,
                                new NoticeContents(moduleName, Collections.emptyList())))
                .containsOnlyKeys(NoticeFileChecker.Severity.CRITICAL);
    }
}
