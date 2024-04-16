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

package org.apache.flink.fs.s3.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.apache.flink.util.Preconditions;

import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ACCESS_KEY;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ENDPOINT;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_EXTRA_ARGS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_PATH;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.SECRET_KEY;

/** Unit tests for FlinkS3FileSystem. */
class FlinkS3FileSystemTest {
    @TempDir public static File temporaryDirectory;

    @Test
    @DisabledOnOs({OS.WINDOWS, OS.OTHER}) // OS must support SLEEP command
    public void testCopyCommandInterruptible() throws Exception {

        File cmdFile = new File(temporaryDirectory, "cmd");

        FileUtils.writeStringToFile(cmdFile, "sleep 1000", Charset.defaultCharset());

        Preconditions.checkState(cmdFile.setExecutable(true), "Cannot set script file executable.");

        final Configuration conf = new Configuration();
        conf.set(S5CMD_PATH, cmdFile.getAbsolutePath());
        conf.set(S5CMD_EXTRA_ARGS, "");
        conf.set(ACCESS_KEY, "test-access-key");
        conf.set(SECRET_KEY, "test-secret-key");
        conf.set(ENDPOINT, "test-endpoint");

        TestS3FileSystemFactory factory = new TestS3FileSystemFactory();
        factory.configure(conf);

        FlinkS3FileSystem fs = (FlinkS3FileSystem) factory.create(new URI("s3://test"));

        AtomicReference<IOException> actualException = new AtomicReference<>();
        CountDownLatch registerLatch = new CountDownLatch(1);
        ICloseableRegistry closeableRegistry =
                new CloseableRegistry() {
                    @Override
                    protected void doRegister(
                            @Nonnull Closeable closeable,
                            @Nonnull Map<Closeable, Object> closeableMap) {
                        super.doRegister(closeable, closeableMap);
                        registerLatch.countDown();
                    }
                };

        Thread thread =
                new Thread(
                        () -> {
                            try {
                                fs.copyFiles(
                                        Collections.singletonList(
                                                new PathsCopyingFileSystem.CopyTask(
                                                        Path.fromLocalFile(new File("")),
                                                        Path.fromLocalFile(new File("")))),
                                        closeableRegistry);
                            } catch (IOException ex) {
                                actualException.set(ex);
                            }
                        });

        thread.start();
        registerLatch.await();
        closeableRegistry.close();
        thread.join(60_000L);
        Assertions.assertThat(thread.isAlive()).isFalse();
        Assertions.assertThat(actualException.get())
                .hasStackTraceContaining("Copy process destroyed by CloseableRegistry.");
    }
}
