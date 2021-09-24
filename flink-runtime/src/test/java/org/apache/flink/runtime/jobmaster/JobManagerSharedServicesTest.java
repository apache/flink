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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Tests auxiliary shared services created by {@link JobManagerSharedServices} and used by the
 * {@link JobMaster}.
 */
public class JobManagerSharedServicesTest extends TestLogger {

    private static final int CPU_CORES = Hardware.getNumberCPUCores();

    @Test
    public void testFutureExecutorNoConfiguration() throws Exception {
        final Configuration config = new Configuration();

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        try {
            ScheduledExecutorService futureExecutor = jobManagerSharedServices.getFutureExecutor();

            assertExecutorPoolSize(futureExecutor, CPU_CORES);
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    @Test
    public void testFutureExecutorConfiguration() throws Exception {
        final int futurePoolSize = 8;
        final Configuration config = new Configuration();
        config.setInteger(JobManagerOptions.JOB_MANAGER_FUTURE_POOL_SIZE, futurePoolSize);

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        assertExecutorPoolSize(jobManagerSharedServices.getFutureExecutor(), futurePoolSize);

        jobManagerSharedServices.shutdown();
    }

    @Test
    public void testIoExecutorNoConfiguration() throws Exception {
        final Configuration config = new Configuration();

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        try {
            assertExecutorPoolSize(jobManagerSharedServices.getIoExecutor(), CPU_CORES);
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    @Test
    public void testIoExecutorConfiguration() throws Exception {
        final int ioPoolSize = 5;
        final Configuration config = new Configuration();
        config.setInteger(JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE, ioPoolSize);

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        try {
            assertExecutorPoolSize(jobManagerSharedServices.getIoExecutor(), ioPoolSize);
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    @Nonnull
    private JobManagerSharedServices buildJobManagerSharedServices(Configuration configuration)
            throws Exception {
        return JobManagerSharedServices.fromConfiguration(
                configuration,
                new BlobServer(configuration, new VoidBlobStore()),
                new TestingFatalErrorHandler());
    }

    private void assertExecutorPoolSize(Executor executor, int expectedPoolSize)
            throws InterruptedException {
        final CountDownLatch expectedPoolSizeLatch = new CountDownLatch(expectedPoolSize);
        final int expectedPoolSizePlusOne = expectedPoolSize + 1;
        final CountDownLatch expectedPoolSizePlusOneLatch =
                new CountDownLatch(expectedPoolSizePlusOne);
        final OneShotLatch releaseLatch = new OneShotLatch();

        ThrowingRunnable<Exception> countsDown =
                () -> {
                    expectedPoolSizePlusOneLatch.countDown();
                    expectedPoolSizeLatch.countDown();
                    // block the runnable to keep the thread occupied
                    releaseLatch.await();
                };

        for (int i = 0; i < expectedPoolSizePlusOne; i++) {
            executor.execute(ThrowingRunnable.unchecked(countsDown));
        }

        // the expected pool size latch should complete since we expect to have enough threads
        expectedPoolSizeLatch.await();
        assertEquals(1, expectedPoolSizePlusOneLatch.getCount());

        // unblock the runnables
        releaseLatch.trigger();
    }
}
