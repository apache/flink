/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_DIR_PREFIX;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME;
import static org.junit.Assert.assertThat;

/** Tests if the coordinator handles up and downscaling. */
public class CoordinatedSourceRescaleITCase extends TestLogger {

    public static final String CREATED_CHECKPOINT = "successfully created checkpoint";
    public static final String RESTORED_CHECKPOINT = "successfully restored checkpoint";

    @Rule public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testDownscaling() throws Exception {
        final File checkpointDir = temp.newFolder();
        final File lastCheckpoint = generateCheckpoint(checkpointDir, 7);
        resumeCheckpoint(checkpointDir, lastCheckpoint, 3);
    }

    @Test
    public void testUpscaling() throws Exception {
        final File checkpointDir = temp.newFolder();
        final File lastCheckpoint = generateCheckpoint(checkpointDir, 3);
        resumeCheckpoint(checkpointDir, lastCheckpoint, 7);
    }

    private File generateCheckpoint(File checkpointDir, int p) throws IOException {
        final StreamExecutionEnvironment env = createEnv(checkpointDir, null, p);

        try {
            env.execute("create checkpoint");
            throw new AssertionError("No checkpoint");
        } catch (Exception e) {
            assertThat(e, FlinkMatchers.containsMessage(CREATED_CHECKPOINT));
            return Files.find(checkpointDir.toPath(), 2, this::isCompletedCheckpoint)
                    .max(Comparator.comparing(Path::toString))
                    .map(Path::toFile)
                    .orElseThrow(() -> new IllegalStateException("Cannot generate checkpoint", e));
        }
    }

    private boolean isCompletedCheckpoint(Path path, BasicFileAttributes attr) {
        return attr.isDirectory()
                && path.getFileName().toString().startsWith(CHECKPOINT_DIR_PREFIX)
                && Files.exists(path.resolve(METADATA_FILE_NAME));
    }

    private void resumeCheckpoint(File checkpointDir, File restoreCheckpoint, int p) {
        final StreamExecutionEnvironment env = createEnv(checkpointDir, restoreCheckpoint, p);

        try {
            env.execute("resume checkpoint");
            throw new AssertionError("No success error");
        } catch (Exception e) {
            assertThat(e, FlinkMatchers.containsMessage(RESTORED_CHECKPOINT));
        }
    }

    private StreamExecutionEnvironment createEnv(
            File checkpointDir, @Nullable File restoreCheckpoint, int p) {
        Configuration conf = new Configuration();
        conf.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
        if (restoreCheckpoint != null) {
            conf.set(SavepointConfigOptions.SAVEPOINT_PATH, restoreCheckpoint.toURI().toString());
        }
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, p);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(p, conf);
        env.enableCheckpointing(100);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<Long> stream = env.fromSequence(0, Long.MAX_VALUE);
        stream.map(new FailingMapFunction(restoreCheckpoint == null)).addSink(new SleepySink());

        return env;
    }

    private static class FailingMapFunction extends RichMapFunction<Long, Long>
            implements CheckpointListener {
        private static final long serialVersionUID = 699621912578369378L;
        private final boolean generateCheckpoint;
        private boolean processedRecord;

        FailingMapFunction(boolean generateCheckpoint) {
            this.generateCheckpoint = generateCheckpoint;
        }

        @Override
        public Long map(Long value) throws Exception {
            processedRecord = true;
            // run a bit before failing
            if (!generateCheckpoint && value % 100 == 42) {
                throw new Exception(RESTORED_CHECKPOINT);
            }
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (generateCheckpoint && processedRecord && checkpointId > 5) {
                throw new Exception(CREATED_CHECKPOINT);
            }
        }
    }

    private static class SleepySink implements SinkFunction<Long> {
        private static final long serialVersionUID = -3542950841846119765L;

        @Override
        public void invoke(Long value, Context context) throws Exception {
            if (value % 1000 == 0) {
                Thread.sleep(1);
            }
        }
    }
}
