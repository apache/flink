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
import org.apache.flink.core.fs.PathsCopyingFileSystem.CopyRequest;
import org.apache.flink.util.Preconditions;

import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ACCESS_KEY;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ENDPOINT;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_BATCH_MAX_FILES;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_EXTRA_ARGS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_PATH;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.SECRET_KEY;

/** Unit tests for FlinkS3FileSystem. */
class FlinkS3FileSystemTest {

    @Test
    @DisabledOnOs({OS.WINDOWS, OS.OTHER}) // OS must support SLEEP command
    public void testCopyCommandInterruptible(@TempDir File temporaryDirectory) throws Exception {

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
                                                CopyRequest.of(
                                                        Path.fromLocalFile(new File("")),
                                                        Path.fromLocalFile(new File("")),
                                                        100L)),
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

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 10, 14, Integer.MAX_VALUE})
    @EnabledOnOs({OS.LINUX, OS.MAC}) // POSIX OS only to run shell script
    public void testCopyCommandBatching(int batchSize, @TempDir File temporaryDirectory)
            throws Exception {
        final int numFiles = 10;

        File cmdFile = new File(temporaryDirectory, "cmd");
        File inputToCmd = new File(temporaryDirectory, "input");
        Preconditions.checkState(inputToCmd.mkdir());

        String cmd =
                String.format(
                        "file=$(mktemp %s/s5cmd-input-XXX)\n"
                                + "while read line; do echo $line >> $file; done < /dev/stdin",
                        inputToCmd.getAbsolutePath());

        FileUtils.writeStringToFile(cmdFile, cmd);
        Preconditions.checkState(cmdFile.setExecutable(true), "Cannot set script file executable.");

        final Configuration conf = new Configuration();
        conf.set(S5CMD_PATH, cmdFile.getAbsolutePath());
        conf.set(S5CMD_EXTRA_ARGS, "");
        conf.set(S5CMD_BATCH_MAX_FILES, batchSize);
        conf.set(ACCESS_KEY, "test-access-key");
        conf.set(SECRET_KEY, "test-secret-key");
        conf.set(ENDPOINT, "test-endpoint");

        TestS3FileSystemFactory factory = new TestS3FileSystemFactory();
        factory.configure(conf);

        FlinkS3FileSystem fs = (FlinkS3FileSystem) factory.create(new URI("s3://test"));
        List<CopyRequest> tasks =
                IntStream.range(0, numFiles)
                        .mapToObj(
                                i ->
                                        CopyRequest.of(
                                                new Path("file:///src-" + i),
                                                new Path("file:///dst-" + i),
                                                123L))
                        .collect(Collectors.toList());
        fs.copyFiles(tasks, ICloseableRegistry.NO_OP);

        File[] files = inputToCmd.listFiles();
        Assertions.assertThat(files).isNotNull();
        Assertions.assertThat(files.length)
                .describedAs("Wrong number of s5cmd subcommand batches for input tasks: %s", tasks)
                .isEqualTo(numFiles / batchSize + (numFiles % batchSize > 0 ? 1 : 0));
        int totalSubcommands = 0;
        for (File file : files) {
            List<String> subcommands = FileUtils.readLines(file, StandardCharsets.UTF_8);
            Assertions.assertThat(subcommands.size())
                    .describedAs(
                            "Too many s5cmd subcommands issued per batch: %s\n(input files: %s)",
                            subcommands, tasks)
                    .isLessThanOrEqualTo(batchSize);
            totalSubcommands += subcommands.size();
        }
        Assertions.assertThat(totalSubcommands)
                .describedAs(
                        "The total number of issued s5cmd subcommands did not match the number of copy tasks:\n%s,\n%s",
                        totalSubcommands, tasks)
                .isEqualTo(numFiles);
    }
}
