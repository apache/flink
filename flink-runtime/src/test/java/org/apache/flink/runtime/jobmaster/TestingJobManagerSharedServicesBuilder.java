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

import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.librarycache.ContextClassLoaderLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleTestUtils;
import org.apache.flink.runtime.testutils.TestingUtils;

import java.util.concurrent.ScheduledExecutorService;

/** Builder for the {@link JobManagerSharedServices}. */
public class TestingJobManagerSharedServicesBuilder {

    private ScheduledExecutorService futureExecutor;

    private ScheduledExecutorService ioExecutor;

    private LibraryCacheManager libraryCacheManager;

    private ShuffleMaster<?> shuffleMaster;

    private BlobWriter blobWriter;

    public TestingJobManagerSharedServicesBuilder() {
        futureExecutor = TestingUtils.defaultExecutor();
        ioExecutor = TestingUtils.defaultExecutor();
        libraryCacheManager = ContextClassLoaderLibraryCacheManager.INSTANCE;
        shuffleMaster = ShuffleTestUtils.DEFAULT_SHUFFLE_MASTER;
        blobWriter = VoidBlobWriter.getInstance();
    }

    public TestingJobManagerSharedServicesBuilder futureExecutor(
            ScheduledExecutorService futureExecutor) {
        this.futureExecutor = futureExecutor;
        return this;
    }

    public TestingJobManagerSharedServicesBuilder ioExecutor(ScheduledExecutorService ioExecutor) {
        this.ioExecutor = ioExecutor;
        return this;
    }

    public TestingJobManagerSharedServicesBuilder setShuffleMaster(ShuffleMaster<?> shuffleMaster) {
        this.shuffleMaster = shuffleMaster;
        return this;
    }

    public TestingJobManagerSharedServicesBuilder setLibraryCacheManager(
            LibraryCacheManager libraryCacheManager) {
        this.libraryCacheManager = libraryCacheManager;
        return this;
    }

    public void setBlobWriter(BlobWriter blobWriter) {
        this.blobWriter = blobWriter;
    }

    public JobManagerSharedServices build() {
        return new JobManagerSharedServices(
                futureExecutor, ioExecutor, libraryCacheManager, shuffleMaster, blobWriter);
    }
}
