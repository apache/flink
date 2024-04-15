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
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class EnvironmentInformationTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testJavaMemory() {
        try {
            long fullHeap = EnvironmentInformation.getMaxJvmHeapMemory();
            long freeWithGC = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag();

            assertThat(fullHeap).isGreaterThan(0);
            assertThat(freeWithGC).isGreaterThanOrEqualTo(0);

            try {
                long free = EnvironmentInformation.getSizeOfFreeHeapMemory();
                assertThat(free).isGreaterThanOrEqualTo(0);
            } catch (RuntimeException e) {
                // this may only occur if the Xmx is not set
                assertThat(EnvironmentInformation.getMaxJvmHeapMemory()).isEqualTo(Long.MAX_VALUE);
            }

            // we cannot make these assumptions, because the test JVM may grow / shrink during the
            // GC
            // assertTrue(free <= fullHeap);
            // assertTrue(freeWithGC <= fullHeap);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEnvironmentMethods() {
        try {
            assertThat(EnvironmentInformation.getJvmStartupOptions()).isNotNull();
            assertThat(EnvironmentInformation.getJvmStartupOptionsArray()).isNotNull();
            assertThat(EnvironmentInformation.getJvmVersion()).isNotNull();
            assertThat(EnvironmentInformation.getRevisionInformation()).isNotNull();
            assertThat(EnvironmentInformation.getVersion()).isNotNull();
            assertThat(EnvironmentInformation.getScalaVersion()).isNotNull();
            assertThat(EnvironmentInformation.getBuildTime()).isNotNull();
            assertThat(EnvironmentInformation.getBuildTimeString()).isNotNull();
            assertThat(EnvironmentInformation.getGitCommitId()).isNotNull();
            assertThat(EnvironmentInformation.getGitCommitIdAbbrev()).isNotNull();
            assertThat(EnvironmentInformation.getGitCommitTime()).isNotNull();
            assertThat(EnvironmentInformation.getGitCommitTimeString()).isNotNull();
            assertThat(EnvironmentInformation.getHadoopVersionString()).isNotNull();
            assertThat(EnvironmentInformation.getHadoopUser()).isNotNull();
            assertThat(EnvironmentInformation.getOpenFileHandlesLimit()).isGreaterThanOrEqualTo(-1);

            if (log.isInfoEnabled()) {
                // Visual inspection of the available Environment variables
                // To actually see it set "rootLogger.level = INFO" in "log4j2-test.properties"
                log.info(
                        "JvmStartupOptions      : {}",
                        EnvironmentInformation.getJvmStartupOptions());
                log.info(
                        "JvmStartupOptionsArray : {}",
                        Arrays.asList(EnvironmentInformation.getJvmStartupOptionsArray()));
                log.info("JvmVersion             : {}", EnvironmentInformation.getJvmVersion());
                log.info(
                        "RevisionInformation    : {}",
                        EnvironmentInformation.getRevisionInformation());
                log.info("Version                : {}", EnvironmentInformation.getVersion());
                log.info("ScalaVersion           : {}", EnvironmentInformation.getScalaVersion());
                log.info("BuildTime              : {}", EnvironmentInformation.getBuildTime());
                log.info(
                        "BuildTimeString        : {}", EnvironmentInformation.getBuildTimeString());
                log.info("GitCommitId            : {}", EnvironmentInformation.getGitCommitId());
                log.info(
                        "GitCommitIdAbbrev      : {}",
                        EnvironmentInformation.getGitCommitIdAbbrev());
                log.info("GitCommitTime          : {}", EnvironmentInformation.getGitCommitTime());
                log.info(
                        "GitCommitTimeString    : {}",
                        EnvironmentInformation.getGitCommitTimeString());
                log.info(
                        "HadoopVersionString    : {}",
                        EnvironmentInformation.getHadoopVersionString());
                log.info("HadoopUser             : {}", EnvironmentInformation.getHadoopUser());
                log.info(
                        "OpenFileHandlesLimit   : {}",
                        EnvironmentInformation.getOpenFileHandlesLimit());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testLogEnvironmentInformation() {
        try {
            Logger mockLogger = Mockito.mock(Logger.class);
            EnvironmentInformation.logEnvironmentInfo(mockLogger, "test", new String[0]);
            EnvironmentInformation.logEnvironmentInfo(mockLogger, "test", null);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
