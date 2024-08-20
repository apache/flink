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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CompressionUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_EXTRA_ARGS;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code HAJobRunOnMinioS3StoreITCase} covers a job run where the HA data is stored in Minio. The
 * implementation verifies whether the {@code JobResult} was written into the FileSystem-backed
 * {@code JobResultStore}.
 */
@ExtendWith(TestLoggerExtension.class)
public abstract class S5CmdOnMinioITCase {

    private static final int CHECKPOINT_INTERVAL = 100;

    @RegisterExtension
    @Order(1)
    private static final AllCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new AllCallbackWrapper<>(new TestContainerExtension<>(MinioTestContainer::new));

    @RegisterExtension
    @Order(2)
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    () -> {
                        final Configuration configuration = createConfiguration();
                        FileSystem.initialize(configuration, null);
                        return new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(4)
                                .setConfiguration(configuration)
                                .build();
                    });

    private static Configuration createConfiguration() {
        final Configuration config = new Configuration();
        getMinioContainer().setS3ConfigOptions(config);
        File credentialsFile = new File(temporaryDirectory, "credentials");

        try {
            // It looks like on the CI machines s5cmd by default is using some other default
            // authentication mechanism, that takes precedence over passing secret and access keys
            // via environment variables. For example maybe there exists a credentials file in the
            // default location with secrets from the S3, not MinIO. To circumvent it, lets use our
            // own credentials file with secrets for MinIO.
            checkState(credentialsFile.createNewFile());
            getMinioContainer().writeCredentialsFile(credentialsFile);
            config.set(
                    S5CMD_EXTRA_ARGS,
                    S5CMD_EXTRA_ARGS.defaultValue()
                            + " --credentials-file "
                            + credentialsFile.getAbsolutePath());
            config.set(AbstractS3FileSystemFactory.S5CMD_PATH, getS5CmdPath());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        config.set(CHECKPOINTS_DIRECTORY, createS3URIWithSubPath("checkpoints"));
        // Effectively disable using ByteStreamStateHandle to ensure s5cmd is being used
        config.set(FS_SMALL_FILE_THRESHOLD, MemorySize.parse("1b"));
        return config;
    }

    @TempDir public static File temporaryDirectory;

    private static MinioTestContainer getMinioContainer() {
        return MINIO_EXTENSION.getCustomExtension().getTestContainer();
    }

    @BeforeAll
    public static void prepareS5Cmd() throws Exception {
        Path s5CmdTgz = Paths.get(temporaryDirectory.getPath(), "s5cmd.tar.gz");
        MessageDigest md = MessageDigest.getInstance("MD5");

        final URI uri;
        String expectedMd5;
        switch (OperatingSystem.getCurrentOperatingSystem()) {
            case LINUX:
                uri =
                        new URI(
                                "https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz");
                expectedMd5 = "66549a8bef5183f6ee65bf793aefca0e";
                break;
            case MAC_OS:
                uri =
                        new URI(
                                "https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_macOS-64bit.tar.gz");
                expectedMd5 = "c90292139a9bb8e6643f8970d858c7b9";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported operating system [%s] for this test.",
                                OperatingSystem.getCurrentOperatingSystem()));
        }

        try (InputStream inputStream = uri.toURL().openStream()) {
            DigestInputStream digestInputStream = new DigestInputStream(inputStream, md);
            Files.copy(digestInputStream, s5CmdTgz, StandardCopyOption.REPLACE_EXISTING);
        }
        String actualMd5 = digestToHexMd5(md.digest());
        checkState(
                expectedMd5.equals(actualMd5),
                "Expected md5 [%s] and actual md5 [%s] differ",
                expectedMd5,
                actualMd5);

        CompressionUtils.extractTarFile(s5CmdTgz.toString(), temporaryDirectory.getPath());
    }

    private static String digestToHexMd5(byte[] digest) {
        StringBuffer sb = new StringBuffer();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    @AfterAll
    public static void unsetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Test
    public void testS5CmdConfigurationIsUsed(@InjectMiniCluster MiniCluster flinkCluster)
            throws Exception {
        String moveFrom = getS5CmdPath();
        String moveTo = moveFrom + "-moved";
        new File(moveFrom).renameTo(new File(moveTo));

        try {
            testRecoveryWithS5Cmd(flinkCluster);
        } catch (Exception e) {
            ExceptionUtils.assertThrowable(
                    e, throwable -> throwable.getMessage().contains("Unable to find s5cmd"));
        } finally {
            new File(moveTo).renameTo(new File(moveFrom));
        }
    }

    @Test
    public void testRecoveryWithS5Cmd(@InjectMiniCluster MiniCluster flinkCluster)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(CHECKPOINT_INTERVAL);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // Disable changelog, to make sure state is stored in the RocksDB, not in changelog, as
        // currently only RocksDB is using s5cmd.
        env.enableChangelogStateBackend(false);

        try (CloseableIterator<Record> results =
                env.addSource(new FailingSource())
                        .keyBy(x -> x.key)
                        .reduce(
                                (ReduceFunction<Record>)
                                        (record1, record2) -> {
                                            checkState(record1.key == record2.key);
                                            return new Record(
                                                    record1.key, record1.value + record2.value);
                                        })
                        .collectAsync()) {

            env.execute();

            // verify that the max emitted values are exactly the sum of all emitted records
            long maxValue1 = 0;
            long maxValue2 = 0;
            while (results.hasNext()) {
                Record next = results.next();
                if (next.key == FailingSource.FIRST_KEY) {
                    maxValue1 = Math.max(maxValue1, next.value);
                } else if (next.key == FailingSource.SECOND_KEY) {
                    maxValue2 = Math.max(maxValue2, next.value);
                } else {
                    throw new Exception("This shouldn't happen: " + next);
                }
            }
            assertThat(maxValue1)
                    .isEqualTo(
                            (FailingSource.LAST_EMITTED_VALUE + 1)
                                    * FailingSource.LAST_EMITTED_VALUE
                                    / 2);
            assertThat(maxValue2)
                    .isEqualTo(
                            (FailingSource.LAST_EMITTED_VALUE + 1)
                                    * FailingSource.LAST_EMITTED_VALUE);
        }
    }

    /** Test record. */
    public static class Record {

        public Record() {
            this(0, 0);
        }

        public Record(int key, int value) {
            this.key = key;
            this.value = value;
        }

        public int key;
        public int value;

        @Override
        public String toString() {
            return String.format("%s(key=%d, value=%d)", Record.class.getSimpleName(), key, value);
        }
    }

    /**
     * Bounded source that emits incremental records at most once per checkpoint. Generates a
     * failure once halfway to the end.
     */
    private class FailingSource extends RichSourceFunction<Record> implements CheckpointedFunction {
        public static final int FIRST_KEY = 1;
        public static final int SECOND_KEY = 2;

        public static final int LAST_EMITTED_VALUE = 10;
        private static final int FAIL_AFTER_VALUE = LAST_EMITTED_VALUE / 2;

        private ListState<Integer> lastEmittedValueState;
        private int lastEmittedValue;
        private boolean isRestored = false;
        private boolean emitted = false;

        private volatile boolean running = true;

        @Override
        public void run(SourceContext ctx) throws Exception {
            while (running && lastEmittedValue < LAST_EMITTED_VALUE) {
                synchronized (ctx.getCheckpointLock()) {
                    if (!emitted) {
                        lastEmittedValue += 1;
                        ctx.collect(new Record(FIRST_KEY, lastEmittedValue));
                        ctx.collect(new Record(SECOND_KEY, 2 * lastEmittedValue));
                        emitted = true;
                    }
                }
                Thread.sleep(CHECKPOINT_INTERVAL / 20 + 1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!isRestored && lastEmittedValue > FAIL_AFTER_VALUE) {
                throw new ExpectedTestException("Time to failover!");
            }
            lastEmittedValueState.update(Collections.singletonList(lastEmittedValue));
            emitted = false;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            lastEmittedValueState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("lastEmittedValue", Integer.class));

            Iterator<Integer> lastEmittedValues = lastEmittedValueState.get().iterator();
            if (lastEmittedValues.hasNext()) {
                lastEmittedValue = lastEmittedValues.next();
                isRestored = true;
            }
            checkState(!lastEmittedValues.hasNext());
        }
    }

    private static String createS3URIWithSubPath(String... subfolders) {
        return getMinioContainer().getS3UriForDefaultBucket() + createSubPath(subfolders);
    }

    private static String createSubPath(String... subfolders) {
        final String pathSeparator = "/";
        return pathSeparator + StringUtils.join(subfolders, pathSeparator);
    }

    private static String getS5CmdPath() {
        return Paths.get(temporaryDirectory.getPath(), "s5cmd").toAbsolutePath().toString();
    }
}
