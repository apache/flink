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

package org.apache.flink.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.TestingClientClassLoaderFactoryBuilder.TestingURLClassLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;

/** Test for the ClientUtils. */
public class ClientUtilsTest extends TestLogger {

    private static final JobID TESTING_JOB_ID = new JobID();

    /**
     * Ensure that the waitUntilJobInitializationFinished() method throws
     * JobInitializationException.
     */
    @Test
    public void testWaitUntilJobInitializationFinished_throwsInitializationException() {
        Iterator<JobStatus> statusSequenceIterator =
                Arrays.asList(JobStatus.INITIALIZING, JobStatus.INITIALIZING, JobStatus.FAILED)
                        .iterator();

        CommonTestUtils.assertThrows(
                "Something is wrong",
                JobInitializationException.class,
                () -> {
                    ClientUtils.waitUntilJobInitializationFinished(
                            statusSequenceIterator::next,
                            () -> {
                                Throwable throwable =
                                        new JobInitializationException(
                                                TESTING_JOB_ID,
                                                "Something is wrong",
                                                new RuntimeException("Err"));
                                return buildJobResult(throwable);
                            },
                            ClassLoader.getSystemClassLoader());
                    return null;
                });
    }

    /**
     * Ensure that waitUntilJobInitializationFinished() does not throw non-initialization
     * exceptions.
     */
    @Test
    public void testWaitUntilJobInitializationFinished_doesNotThrowRuntimeException()
            throws Exception {
        Iterator<JobStatus> statusSequenceIterator =
                Arrays.asList(JobStatus.INITIALIZING, JobStatus.INITIALIZING, JobStatus.FAILED)
                        .iterator();
        ClientUtils.waitUntilJobInitializationFinished(
                statusSequenceIterator::next,
                () -> buildJobResult(new RuntimeException("Err")),
                ClassLoader.getSystemClassLoader());
    }

    /** Ensure that other errors are thrown. */
    @Test
    public void testWaitUntilJobInitializationFinished_throwsOtherErrors() {
        CommonTestUtils.assertThrows(
                "Error while waiting for job to be initialized",
                RuntimeException.class,
                () -> {
                    ClientUtils.waitUntilJobInitializationFinished(
                            () -> {
                                throw new RuntimeException("other error");
                            },
                            () -> {
                                Throwable throwable =
                                        new JobInitializationException(
                                                TESTING_JOB_ID,
                                                "Something is wrong",
                                                new RuntimeException("Err"));
                                return buildJobResult(throwable);
                            },
                            ClassLoader.getSystemClassLoader());
                    return null;
                });
    }

    private JobResult buildJobResult(Throwable throwable) {
        return new JobResult.Builder()
                .jobId(TESTING_JOB_ID)
                .serializedThrowable(new SerializedThrowable(throwable))
                .netRuntime(1)
                .build();
    }

    /** Test normal operation. */
    @Test
    public void testWaitUntilJobInitializationFinished_regular() throws Exception {
        Iterator<JobStatus> statusSequenceIterator =
                Arrays.asList(JobStatus.INITIALIZING, JobStatus.INITIALIZING, JobStatus.RUNNING)
                        .iterator();
        ClientUtils.waitUntilJobInitializationFinished(
                statusSequenceIterator::next,
                () -> {
                    Assert.fail("unexpected call");
                    return null;
                },
                ClassLoader.getSystemClassLoader());
    }

    @Test
    public void testClassLoader_by_customized_classLoaderFactory() throws Exception {
        System.getProperties()
                .setProperty(
                        TestingClientClassLoaderFactoryBuilder
                                .TESTING_CLASSLOADER_FACTORY_BUILDER_ENABLE,
                        Boolean.TRUE.toString());
        URLClassLoader classLoader = null;
        try {
            Configuration configuration = new Configuration();
            classLoader =
                    ClientUtils.buildUserCodeClassLoader(
                            Collections.emptyList(),
                            Collections.emptyList(),
                            this.getClass().getClassLoader(),
                            configuration);
            assertTrue(
                    "The impl class must be " + TestingURLClassLoader.class.getName(),
                    classLoader instanceof TestingURLClassLoader);
        } finally {
            System.clearProperty(
                    TestingClientClassLoaderFactoryBuilder
                            .TESTING_CLASSLOADER_FACTORY_BUILDER_ENABLE);
        }
    }

    @Test
    public void testClassLoader_without_customized_classLoaderFactory() {
        URLClassLoader classLoader = null;

        Configuration configuration = new Configuration();
        configuration.setBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        classLoader =
                ClientUtils.buildUserCodeClassLoader(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        this.getClass().getClassLoader(),
                        configuration);
        assertTrue(
                "The impl class must be " + FlinkUserCodeClassLoader.class.getName(),
                classLoader instanceof FlinkUserCodeClassLoader);
    }
}
