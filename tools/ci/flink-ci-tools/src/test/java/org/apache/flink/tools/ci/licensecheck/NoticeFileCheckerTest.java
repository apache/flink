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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NoticeFileCheckerTest {
    @Test
    void testRunHappyPath() throws IOException {
        final String moduleName = "test";
        final Dependency bundledDependency = Dependency.create("a", "b", "c", null);
        final Map<String, Set<Dependency>> bundleDependencies = new HashMap<>();
        bundleDependencies.put(moduleName, Collections.singleton(bundledDependency));
        final Set<String> deployedModules = Collections.singleton(moduleName);
        final Optional<NoticeContents> noticeContents =
                Optional.of(
                        new NoticeContents(
                                moduleName, Collections.singletonList(bundledDependency)));

        assertThat(
                        NoticeFileChecker.run(
                                bundleDependencies,
                                deployedModules,
                                Collections.singletonMap(moduleName, noticeContents)))
                .isEqualTo(0);
    }

    @Test
    void testRunRejectsMissingNotice() throws IOException {
        final String moduleName = "test";
        final Dependency bundledDependency = Dependency.create("a", "b", "c", null);
        final Map<String, Set<Dependency>> bundleDependencies = new HashMap<>();
        bundleDependencies.put(moduleName, Collections.singleton(bundledDependency));
        final Set<String> deployedModules = Collections.singleton(moduleName);
        final Optional<NoticeContents> missingNotice = Optional.empty();

        assertThat(
                        NoticeFileChecker.run(
                                bundleDependencies,
                                deployedModules,
                                Collections.singletonMap(moduleName, missingNotice)))
                .isEqualTo(1);
    }

    @Test
    void testRunRejectsIncorrectNotice() throws IOException {
        final String moduleName = "test";
        final Dependency bundledDependency = Dependency.create("a", "b", "c", null);
        final Map<String, Set<Dependency>> bundleDependencies = new HashMap<>();
        bundleDependencies.put(moduleName, Collections.singleton(bundledDependency));
        final Set<String> deployedModules = Collections.singleton(moduleName);
        final Optional<NoticeContents> emptyNotice =
                Optional.of(new NoticeContents(moduleName, Collections.emptyList()));

        assertThat(
                        NoticeFileChecker.run(
                                bundleDependencies,
                                deployedModules,
                                Collections.singletonMap(moduleName, emptyNotice)))
                .isEqualTo(1);
    }

    @Test
    void testRunSkipsNonDeployedModules() throws IOException {
        final String moduleName = "test";
        final Dependency bundledDependency = Dependency.create("a", "b", "c", null);
        final Map<String, Set<Dependency>> bundleDependencies = new HashMap<>();
        bundleDependencies.put(moduleName, Collections.singleton(bundledDependency));
        final Set<String> deployedModules = Collections.emptySet();
        // this would usually be a problem, but since the module is not deployed it's OK!
        final Optional<NoticeContents> emptyNotice =
                Optional.of(new NoticeContents(moduleName, Collections.emptyList()));

        assertThat(
                        NoticeFileChecker.run(
                                bundleDependencies,
                                deployedModules,
                                Collections.singletonMap(moduleName, emptyNotice)))
                .isEqualTo(0);
    }

    @Test
    void testRunIncludesBundledNonDeployedModules() throws IOException {
        final Map<String, Set<Dependency>> bundledDependencies = new HashMap<>();
        final Map<String, Optional<NoticeContents>> notices = new HashMap<>();

        // a module that is not deployed but bundles another dependency with an empty NOTICE
        final String nonDeployedModuleName = "nonDeployed";
        final Dependency nonDeployedDependency =
                Dependency.create("a", nonDeployedModuleName, "c", null);
        final Dependency bundledDependency = Dependency.create("a", "b", "c", null);
        bundledDependencies.put(nonDeployedModuleName, Collections.singleton(bundledDependency));
        // this would usually not be a problem, but since the module is not bundled it's not OK!
        final Optional<NoticeContents> emptyNotice =
                Optional.of(new NoticeContents(nonDeployedModuleName, Collections.emptyList()));
        notices.put(nonDeployedModuleName, emptyNotice);

        // a module that is deploys and bundles the above
        final String bundlingModule = "bundling";
        bundledDependencies.put(bundlingModule, Collections.singleton(nonDeployedDependency));
        final Optional<NoticeContents> correctNotice =
                Optional.of(
                        new NoticeContents(
                                bundlingModule, Collections.singletonList(nonDeployedDependency)));
        notices.put(bundlingModule, correctNotice);

        final Set<String> deployedModules = Collections.singleton(bundlingModule);

        assertThat(NoticeFileChecker.run(bundledDependencies, deployedModules, notices))
                .isEqualTo(1);
    }

    @Test
    void testCheckNoticeFileHappyPath() {
        final String moduleName = "test";
        final Dependency bundledDependency = Dependency.create("a", "b", "c", null);
        final Map<String, Set<Dependency>> bundleDependencies = new HashMap<>();
        bundleDependencies.put(moduleName, Collections.singleton(bundledDependency));

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
        assertThat(NoticeFileChecker.checkNoticeFile(Collections.emptyMap(), "test", null))
                .containsOnlyKeys(NoticeFileChecker.Severity.CRITICAL);
    }

    @Test
    void testCheckNoticeFileToleratesModuleNameMismatch() {
        final String moduleName = "test";

        assertThat(
                        NoticeFileChecker.checkNoticeFile(
                                Collections.emptyMap(),
                                moduleName,
                                new NoticeContents(moduleName + "2", Collections.emptyList())))
                .containsOnlyKeys(NoticeFileChecker.Severity.TOLERATED);
    }

    @Test
    void testCheckNoticeFileRejectsDuplicateLine() {
        final String moduleName = "test";
        final Map<String, Set<Dependency>> bundleDependencies = new HashMap<>();
        bundleDependencies.put(
                moduleName, Collections.singleton(Dependency.create("a", "b", "c", null)));

        assertThat(
                        NoticeFileChecker.checkNoticeFile(
                                bundleDependencies,
                                moduleName,
                                new NoticeContents(
                                        moduleName,
                                        Arrays.asList(
                                                Dependency.create("a", "b", "c", null),
                                                Dependency.create("a", "b", "c", null)))))
                .containsOnlyKeys(NoticeFileChecker.Severity.CRITICAL);
    }

    @Test
    void testCheckNoticeFileRejectsMissingDependency() {
        final String moduleName = "test";
        final Map<String, Set<Dependency>> bundleDependencies = new HashMap<>();
        bundleDependencies.put(
                moduleName, Collections.singleton(Dependency.create("a", "b", "c", null)));

        assertThat(
                        NoticeFileChecker.checkNoticeFile(
                                bundleDependencies,
                                moduleName,
                                new NoticeContents(moduleName, Collections.emptyList())))
                .containsOnlyKeys(NoticeFileChecker.Severity.CRITICAL);
    }
}
