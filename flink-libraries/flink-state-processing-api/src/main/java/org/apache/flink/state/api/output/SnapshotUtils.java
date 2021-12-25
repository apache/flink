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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.MathUtils;

import java.io.IOException;

import static org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD;
import static org.apache.flink.configuration.CheckpointingOptions.FS_WRITE_BUFFER_SIZE;

/** Takes a final snapshot of the state of an operator subtask. */
@Internal
public final class SnapshotUtils {
    static final long CHECKPOINT_ID = 0L;

    private SnapshotUtils() {}

    public static <OUT, OP extends StreamOperator<OUT>> TaggedOperatorSubtaskState snapshot(
            OP operator,
            int index,
            long timestamp,
            boolean isExactlyOnceMode,
            boolean isUnalignedCheckpoint,
            Configuration configuration,
            Path savepointPath)
            throws Exception {

        CheckpointOptions options =
                CheckpointOptions.forConfig(
                        CheckpointType.SAVEPOINT,
                        AbstractFsCheckpointStorageAccess.encodePathAsReference(savepointPath),
                        isExactlyOnceMode,
                        isUnalignedCheckpoint,
                        CheckpointOptions.NO_ALIGNED_CHECKPOINT_TIME_OUT);

        operator.prepareSnapshotPreBarrier(CHECKPOINT_ID);

        CheckpointStreamFactory storage = createStreamFactory(configuration, options);

        OperatorSnapshotFutures snapshotInProgress =
                operator.snapshotState(CHECKPOINT_ID, timestamp, options, storage);

        OperatorSubtaskState state =
                new OperatorSnapshotFinalizer(snapshotInProgress).getJobManagerOwnedState();

        operator.notifyCheckpointComplete(CHECKPOINT_ID);
        return new TaggedOperatorSubtaskState(index, state);
    }

    private static CheckpointStreamFactory createStreamFactory(
            Configuration configuration, CheckpointOptions options) throws IOException {
        final Path path =
                AbstractFsCheckpointStorageAccess.decodePathFromReference(
                        options.getTargetLocation());

        return new FsCheckpointStorageLocation(
                path.getFileSystem(),
                path,
                path,
                path,
                options.getTargetLocation(),
                MathUtils.checkedDownCast(configuration.get(FS_SMALL_FILE_THRESHOLD).getBytes()),
                configuration.get(FS_WRITE_BUFFER_SIZE));
    }
}
