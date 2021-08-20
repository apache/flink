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
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests auxiliary shared services created by {@link JobManagerSharedServices} and used by the
 * {@link JobMaster}.
 */
public class JobManagerSharedServicesTest extends TestLogger {

    private static final int NUM_FUTURE_THREADS = 8;
    private static final int NUM_IO_THREADS = 4;
    private static final int CPU_CORES = Hardware.getNumberCPUCores();

    @Test
    public void testFutureExecutorNoConfiguration() throws Exception {
        Configuration config = new Configuration();

        final JobManagerSharedServices jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        config,
                        new BlobServer(config, new VoidBlobStore()),
                        new TestingFatalErrorHandler());

        CountDownLatch releasedLatch = new CountDownLatch(CPU_CORES);
        CountDownLatch unreleasedLatch = new CountDownLatch(CPU_CORES + 1);
        ScheduledExecutorService futureExecutor = jobManagerSharedServices.getFutureExecutor();
        Runnable countsDown =
                () -> {
                    releasedLatch.countDown();
                    unreleasedLatch.countDown();
                };
        for (int i = 0; i < CPU_CORES; i++) {
            futureExecutor.execute(countsDown);
        }
        // Give the executor enough time to process all the Runnables
        releasedLatch.await(1L, TimeUnit.SECONDS);
        assertEquals(0, releasedLatch.getCount());
        assertEquals(1, unreleasedLatch.getCount());
        jobManagerSharedServices.shutdown();
    }

    @Test
    public void testFutureExecutorConfiguration() throws Exception {
        Configuration config = new Configuration();
        config.setInteger(JobManagerOptions.JOB_MANAGER_FUTURE_THREADS, NUM_FUTURE_THREADS);

        final JobManagerSharedServices jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        config,
                        new BlobServer(config, new VoidBlobStore()),
                        new TestingFatalErrorHandler());

        CountDownLatch releasedLatch = new CountDownLatch(NUM_FUTURE_THREADS);
        CountDownLatch unreleasedLatch = new CountDownLatch(NUM_FUTURE_THREADS + 1);
        ScheduledExecutorService futureExecutor = jobManagerSharedServices.getFutureExecutor();
        Runnable countsDown =
                () -> {
                    releasedLatch.countDown();
                    unreleasedLatch.countDown();
                };
        for (int i = 0; i < NUM_FUTURE_THREADS; i++) {
            futureExecutor.execute(countsDown);
        }
        // Give the executor enough time to process all the Runnables
        releasedLatch.await(1L, TimeUnit.SECONDS);
        assertEquals(0, releasedLatch.getCount());
        assertEquals(1, unreleasedLatch.getCount());
        jobManagerSharedServices.shutdown();
    }

    @Test
    public void testIoExecutorNoConfiguration() throws Exception {
        Configuration config = new Configuration();

        final JobManagerSharedServices jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        config,
                        new BlobServer(config, new VoidBlobStore()),
                        new TestingFatalErrorHandler());

        CountDownLatch releasedLatch = new CountDownLatch(CPU_CORES);
        CountDownLatch unreleasedLatch = new CountDownLatch(CPU_CORES + 1);
        ExecutorService ioExecutor = jobManagerSharedServices.getIoExecutor();
        Runnable countsDown =
                () -> {
                    releasedLatch.countDown();
                    unreleasedLatch.countDown();
                };
        for (int i = 0; i < CPU_CORES; i++) {
            ioExecutor.execute(countsDown);
        }
        // Give the executor enough time to process all the Runnables
        releasedLatch.await(1L, TimeUnit.SECONDS);
        assertEquals(0, releasedLatch.getCount());
        assertEquals(1, unreleasedLatch.getCount());
        jobManagerSharedServices.shutdown();
    }

    @Test
    public void testIoExecutorConfiguration() throws Exception {
        Configuration config = new Configuration();
        config.setInteger(JobManagerOptions.JOB_MANAGER_IO_THREADS, NUM_IO_THREADS);

        final JobManagerSharedServices jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        config,
                        new BlobServer(config, new VoidBlobStore()),
                        new TestingFatalErrorHandler());

        CountDownLatch releasedLatch = new CountDownLatch(NUM_IO_THREADS);
        CountDownLatch unreleasedLatch = new CountDownLatch(NUM_IO_THREADS + 1);
        ExecutorService ioExecutor = jobManagerSharedServices.getIoExecutor();
        Runnable countsDown =
                () -> {
                    releasedLatch.countDown();
                    unreleasedLatch.countDown();
                };
        for (int i = 0; i < NUM_IO_THREADS; i++) {
            ioExecutor.execute(countsDown);
        }
        // Give the executor enough time to process all the Runnables
        releasedLatch.await(1L, TimeUnit.SECONDS);
        assertEquals(0, releasedLatch.getCount());
        assertEquals(1, unreleasedLatch.getCount());
        jobManagerSharedServices.shutdown();
    }
}
