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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshot;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** This class is used to record the state of {@link ExecutionVertex}. */
public class ExecutionVertexFinishedEvent implements JobEvent {
    private final ExecutionAttemptID executionAttemptId;

    private final TaskManagerLocation taskManagerLocation;

    private transient Map<OperatorID, CompletableFuture<byte[]>> operatorCoordinatorSnapshotFutures;

    @Nullable
    private transient CompletableFuture<ShuffleMasterSnapshot> shuffleMasterSnapshotFuture;

    private final IOMetrics ioMetrics;

    @Nullable private final Map<String, Accumulator<?, ?>> userAccumulators;

    public ExecutionVertexFinishedEvent(
            ExecutionAttemptID executionAttemptId,
            TaskManagerLocation location,
            Map<OperatorID, CompletableFuture<byte[]>> operatorCoordinatorSnapshotFutures,
            @Nullable CompletableFuture<ShuffleMasterSnapshot> shuffleMasterSnapshotFuture,
            IOMetrics ioMetrics,
            @Nullable Map<String, Accumulator<?, ?>> userAccumulators) {
        this.executionAttemptId = checkNotNull(executionAttemptId);
        this.taskManagerLocation = checkNotNull(location);
        this.operatorCoordinatorSnapshotFutures = checkNotNull(operatorCoordinatorSnapshotFutures);
        this.shuffleMasterSnapshotFuture = shuffleMasterSnapshotFuture;

        this.ioMetrics = ioMetrics;
        this.userAccumulators = userAccumulators;
    }

    public ExecutionAttemptID getExecutionAttemptId() {
        return executionAttemptId;
    }

    public ExecutionVertexID getExecutionVertexId() {
        return executionAttemptId.getExecutionVertexId();
    }

    public int getAttemptNumber() {
        return executionAttemptId.getAttemptNumber();
    }

    public TaskManagerLocation getTaskManagerLocation() {
        return taskManagerLocation;
    }

    public Map<OperatorID, CompletableFuture<byte[]>> getOperatorCoordinatorSnapshotFutures() {
        return operatorCoordinatorSnapshotFutures;
    }

    @Nullable
    public CompletableFuture<ShuffleMasterSnapshot> getShuffleMasterSnapshotFuture() {
        return shuffleMasterSnapshotFuture;
    }

    public IOMetrics getIOMetrics() {
        return ioMetrics;
    }

    @Nullable
    public Map<String, Accumulator<?, ?>> getUserAccumulators() {
        return userAccumulators;
    }

    public boolean hasOperatorCoordinatorAndShuffleMasterSnapshots() {
        return shuffleMasterSnapshotFuture != null;
    }

    @Override
    public String toString() {
        return "ExecutionVertexFinishedEvent("
                + "executionVertexId='"
                + getExecutionVertexId()
                + "', attemptNumber='"
                + getAttemptNumber()
                + "')";
    }

    /** Serializer for {@link ExecutionVertexFinishedEvent}. */
    static class Serializer implements SimpleVersionedSerializer<JobEvent> {

        private static final int VERSION = 1;

        public static final Serializer INSTANCE = new Serializer();

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(JobEvent jobEvent) throws IOException {
            checkState(
                    jobEvent.getType() == JobEvents.getTypeID(ExecutionVertexFinishedEvent.class));
            ExecutionVertexFinishedEvent event = (ExecutionVertexFinishedEvent) jobEvent;

            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputViewStreamWrapper(byteArrayOutputStream)) {
                // serialize event (exclude operator coordinators and shuffle master).
                byte[] binaryJobEvent = InstantiationUtil.serializeObject(event);
                out.writeInt(binaryJobEvent.length);
                out.write(binaryJobEvent);

                // serialize operator coordinator snapshots.
                Map<OperatorID, byte[]> operatorCoordinatorSnapshots = new HashMap<>();
                for (Map.Entry<OperatorID, CompletableFuture<byte[]>> entry :
                        event.getOperatorCoordinatorSnapshotFutures().entrySet()) {
                    operatorCoordinatorSnapshots.put(entry.getKey(), entry.getValue().get());
                }
                byte[] binaryOperatorCoordinatorSnapshots =
                        InstantiationUtil.serializeObject(operatorCoordinatorSnapshots);
                out.writeInt(binaryOperatorCoordinatorSnapshots.length);
                out.write(binaryOperatorCoordinatorSnapshots);

                // serialize shuffle master state
                if (event.getShuffleMasterSnapshotFuture() != null) {
                    byte[] binaryShuffleMasterSnapshot =
                            InstantiationUtil.serializeObject(
                                    event.getShuffleMasterSnapshotFuture().get());
                    out.writeInt(binaryShuffleMasterSnapshot.length);
                    out.write(binaryShuffleMasterSnapshot);
                } else {
                    out.writeInt(0);
                }

                // flush and return
                out.flush();
                return byteArrayOutputStream.toByteArray();
            } catch (Exception exception) {
                throw new IOException(
                        "Serialize ExecutionVertexFinishedEvent " + event + " failed.", exception);
            }
        }

        @Override
        public JobEvent deserialize(int version, byte[] bytes) throws IOException {
            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                    DataInputStream in = new DataInputViewStreamWrapper(byteArrayInputStream)) {

                // deserialize event (exclude operator coordinators)
                int binaryJobEventSize = in.readInt();
                byte[] binaryJobEvent = readBytes(in, binaryJobEventSize);
                ExecutionVertexFinishedEvent jobEvent =
                        InstantiationUtil.deserializeObject(
                                binaryJobEvent, ClassLoader.getSystemClassLoader());

                // deserialize operator coordinator snapshots.
                int binaryOperatorCoordinatorSnapshotsSize = in.readInt();
                byte[] binaryOperatorCoordinatorSnapshots =
                        readBytes(in, binaryOperatorCoordinatorSnapshotsSize);
                Map<OperatorID, byte[]> operatorCoordinatorSnapshots =
                        InstantiationUtil.deserializeObject(
                                binaryOperatorCoordinatorSnapshots,
                                ClassLoader.getSystemClassLoader());

                // put operator coordinator snapshots in event.
                jobEvent.operatorCoordinatorSnapshotFutures = new HashMap<>();
                operatorCoordinatorSnapshots.forEach(
                        (key, value) ->
                                jobEvent.operatorCoordinatorSnapshotFutures.put(
                                        key, CompletableFuture.completedFuture(value)));

                // deserialize shuffle master snapshot.
                int binaryShuffleMasterSnapshotSize = in.readInt();
                if (binaryShuffleMasterSnapshotSize > 0) {
                    byte[] binaryShuffleMasterSnapshot =
                            readBytes(in, binaryShuffleMasterSnapshotSize);
                    jobEvent.shuffleMasterSnapshotFuture =
                            CompletableFuture.completedFuture(
                                    InstantiationUtil.deserializeObject(
                                            binaryShuffleMasterSnapshot,
                                            ClassLoader.getSystemClassLoader()));
                } else {
                    jobEvent.shuffleMasterSnapshotFuture = null;
                }

                return jobEvent;
            } catch (Exception exception) {
                throw new IOException(
                        "Deserialize ExecutionVertexFinishedEvent failed.", exception);
            }
        }

        private static byte[] readBytes(DataInputStream in, int size) throws IOException {
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            return bytes;
        }
    }
}
