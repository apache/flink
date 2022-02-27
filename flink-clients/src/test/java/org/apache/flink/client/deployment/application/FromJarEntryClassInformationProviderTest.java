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

package org.apache.flink.client.deployment.application;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code FromJarEntryClassInformationProviderTest} tests {@link
 * FromJarEntryClassInformationProvider}.
 */
class FromJarEntryClassInformationProviderTest {

    @Test
    void testCustomJarFile() {
        final File jarFile = new File("some/path/to/jar");
        final String jobClassName = "JobClassName";
        final FromJarEntryClassInformationProvider testInstance =
                FromJarEntryClassInformationProvider.createFromCustomJar(jarFile, jobClassName);

        assertThat(testInstance.getJarFile()).get().isEqualTo(jarFile);
        assertThat(testInstance.getJobClassName()).get().isEqualTo(jobClassName);
    }

    @Test
    void testMissingJar() {
        assertThatThrownBy(
                        () ->
                                FromJarEntryClassInformationProvider.createFromCustomJar(
                                        null, "JobClassName"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testMissingJobClassName() {
        final File jarFile = new File("some/path/to/jar");
        final EntryClassInformationProvider testInstance =
                FromJarEntryClassInformationProvider.createFromCustomJar(jarFile, null);
        assertThat(testInstance.getJarFile()).contains(jarFile);
        assertThat(testInstance.getJobClassName()).isEmpty();
    }

    @Test
    void testEitherJobClassNameOrJarHasToBeSet() {
        assertThatThrownBy(
                        () -> FromJarEntryClassInformationProvider.createFromCustomJar(null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
