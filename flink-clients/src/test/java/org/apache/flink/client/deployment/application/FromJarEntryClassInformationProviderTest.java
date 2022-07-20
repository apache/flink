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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * {@code FromJarEntryClassInformationProviderTest} tests {@link
 * FromJarEntryClassInformationProvider}.
 */
public class FromJarEntryClassInformationProviderTest extends TestLogger {

    @Test
    public void testCustomJarFile() {
        final File jarFile = new File("some/path/to/jar");
        final String jobClassName = "JobClassName";
        final FromJarEntryClassInformationProvider testInstance =
                FromJarEntryClassInformationProvider.createFromCustomJar(jarFile, jobClassName);

        assertThat(testInstance.getJarFile().isPresent(), is(true));
        assertThat(testInstance.getJarFile().get(), is(jarFile));
        assertThat(testInstance.getJobClassName().isPresent(), is(true));
        assertThat(testInstance.getJobClassName().get(), is(jobClassName));
    }

    @Test(expected = NullPointerException.class)
    public void testMissingJar() {
        final EntryClassInformationProvider testInstance =
                FromJarEntryClassInformationProvider.createFromCustomJar(null, "JobClassName");
    }

    @Test
    public void testMissingJobClassName() {
        final File jarFile = new File("some/path/to/jar");
        final EntryClassInformationProvider testInstance =
                FromJarEntryClassInformationProvider.createFromCustomJar(jarFile, null);
        assertThat(testInstance.getJarFile().isPresent(), is(true));
        assertThat(testInstance.getJarFile().get(), is(jarFile));
        assertThat(testInstance.getJobClassName().isPresent(), is(false));
    }

    @Test(expected = NullPointerException.class)
    public void testEitherJobClassNameOrJarHasToBeSet() {
        FromJarEntryClassInformationProvider.createFromCustomJar(null, null);
    }
}
