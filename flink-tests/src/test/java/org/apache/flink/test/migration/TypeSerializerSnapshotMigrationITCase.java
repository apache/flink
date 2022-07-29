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

package org.apache.flink.test.migration;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.checkpointing.utils.MigrationTestUtils;
import org.apache.flink.test.checkpointing.utils.SnapshotMigrationTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Migration IT cases for upgrading a legacy {@link TypeSerializerConfigSnapshot} that is written in
 * checkpoints to {@link TypeSerializerSnapshot} interface.
 *
 * <p>The snapshots used by this test were written with a serializer snapshot class that extends
 * {@link TypeSerializerConfigSnapshot}, as can be seen in the commented out code at the end of this
 * class. On restore, we change the snapshot to implement directly a {@link TypeSerializerSnapshot}.
 */
@RunWith(Parameterized.class)
public class TypeSerializerSnapshotMigrationITCase extends SnapshotMigrationTestBase {

    private static final int NUM_SOURCE_ELEMENTS = 4;

    // TODO increase this to newer version to create and test snapshot migration for newer versions
    private static final FlinkVersion currentVersion = FlinkVersion.v1_15;

    // TODO change this to CREATE_SNAPSHOT to (re)create binary snapshots
    // TODO Note: You should generate the snapshot based on the release branch instead of the
    // master.
    private static final ExecutionMode executionMode = ExecutionMode.VERIFY_SNAPSHOT;

    @Parameterized.Parameters(name = "Test snapshot: {0}")
    public static Collection<SnapshotSpec> parameters() {
        Collection<SnapshotSpec> parameters = new LinkedList<>();
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.MEMORY_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_CANONICAL,
                        FlinkVersion.rangeOf(FlinkVersion.v1_3, FlinkVersion.v1_14)));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_CANONICAL,
                        FlinkVersion.rangeOf(FlinkVersion.v1_15, currentVersion)));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_CANONICAL,
                        FlinkVersion.rangeOf(FlinkVersion.v1_3, currentVersion)));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_NATIVE,
                        FlinkVersion.rangeOf(FlinkVersion.v1_15, currentVersion)));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_NATIVE,
                        FlinkVersion.rangeOf(FlinkVersion.v1_15, currentVersion)));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
                        SnapshotType.CHECKPOINT,
                        FlinkVersion.rangeOf(FlinkVersion.v1_15, currentVersion)));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
                        SnapshotType.CHECKPOINT,
                        FlinkVersion.rangeOf(FlinkVersion.v1_15, currentVersion)));
        if (executionMode == ExecutionMode.CREATE_SNAPSHOT) {
            parameters =
                    parameters.stream()
                            .filter(x -> x.getFlinkVersion().equals(currentVersion))
                            .collect(Collectors.toList());
        }
        return parameters;
    }

    private final SnapshotSpec snapshotSpec;

    public TypeSerializerSnapshotMigrationITCase(SnapshotSpec snapshotSpec) throws Exception {
        this.snapshotSpec = snapshotSpec;
    }

    @Test
    public void testSnapshot() throws Exception {
        final int parallelism = 1;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        switch (snapshotSpec.getStateBackendType()) {
            case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
                env.setStateBackend(new EmbeddedRocksDBStateBackend());
                break;
            case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
                env.setStateBackend(new MemoryStateBackend());
                break;
            case StateBackendLoader.HASHMAP_STATE_BACKEND_NAME:
                env.setStateBackend(new HashMapStateBackend());
                break;
            default:
                throw new UnsupportedOperationException();
        }

        env.enableCheckpointing(500);
        env.setParallelism(parallelism);
        env.setMaxParallelism(parallelism);

        SourceFunction<Tuple2<Long, Long>> nonParallelSource =
                new MigrationTestUtils.CheckpointingNonParallelSourceWithListState(
                        NUM_SOURCE_ELEMENTS);

        env.addSource(nonParallelSource)
                .keyBy(0)
                .map(new TestMapFunction())
                .addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

        final String snapshotPath = getSnapshotPath(snapshotSpec);

        if (executionMode == ExecutionMode.CREATE_SNAPSHOT) {
            executeAndSnapshot(
                    env,
                    "src/test/resources/" + snapshotPath,
                    snapshotSpec.getSnapshotType(),
                    Tuple2.of(
                            MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS));
        } else if (executionMode == ExecutionMode.VERIFY_SNAPSHOT) {
            restoreAndExecute(
                    env,
                    getResourceFilename(snapshotPath),
                    Tuple2.of(
                            MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS));
        } else {
            throw new IllegalStateException("Unknown ExecutionMode " + executionMode);
        }
    }

    private String getSnapshotPath(SnapshotSpec snapshotSpec) {
        return "type-serializer-snapshot-migration-itcase-" + snapshotSpec;
    }

    private static class TestMapFunction
            extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
            ValueState<Long> state =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>("testState", new TestSerializer()));

            state.update(value.f1);
            return value;
        }
    }

    private static class TestSerializer extends TypeSerializer<Long> {

        private static final long serialVersionUID = 1L;

        private LongSerializer serializer = new LongSerializer();

        private String configPayload = "configPayload";

        @Override
        public TypeSerializerSnapshot<Long> snapshotConfiguration() {
            return new TestSerializerSnapshot(configPayload);
        }

        @Override
        public TypeSerializer<Long> duplicate() {
            return this;
        }

        // ------------------------------------------------------------------
        //  Simple forwarded serializer methods
        // ------------------------------------------------------------------

        @Override
        public void serialize(Long record, DataOutputView target) throws IOException {
            serializer.serialize(record, target);
        }

        @Override
        public Long deserialize(Long reuse, DataInputView source) throws IOException {
            return serializer.deserialize(reuse, source);
        }

        @Override
        public Long deserialize(DataInputView source) throws IOException {
            return serializer.deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            serializer.copy(source, target);
        }

        @Override
        public Long copy(Long from) {
            return serializer.copy(from);
        }

        @Override
        public Long copy(Long from, Long reuse) {
            return serializer.copy(from, reuse);
        }

        @Override
        public Long createInstance() {
            return serializer.createInstance();
        }

        @Override
        public boolean isImmutableType() {
            return serializer.isImmutableType();
        }

        @Override
        public int getLength() {
            return serializer.getLength();
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestSerializer;
        }
    }

    /**
     * Test serializer snapshot. The version written in savepoints extend a {@link
     * TypeSerializerConfigSnapshot}.
     */
    public static class TestSerializerSnapshot implements TypeSerializerSnapshot<Long> {

        private String configPayload;

        public TestSerializerSnapshot() {}

        public TestSerializerSnapshot(String configPayload) {
            this.configPayload = configPayload;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public TypeSerializerSchemaCompatibility<Long> resolveSchemaCompatibility(
                TypeSerializer<Long> newSerializer) {
            return (newSerializer instanceof TestSerializer)
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }

        @Override
        public TypeSerializer<Long> restoreSerializer() {
            return new TestSerializer();
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeUTF(configPayload);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readVersion != 1) {
                throw new IllegalStateException("Can not recognize read version: " + readVersion);
            }

            this.configPayload = in.readUTF();
        }
    }

    /**
     * This was the implementation of {@link TestSerializerSnapshot} when the savepoints were
     * written.
     */
    /*
    public static class TestSerializerSnapshot extends TypeSerializerConfigSnapshot {

    	private String configPayload;

    	public TestSerializerSnapshot() {}

    	public TestSerializerSnapshot(String configPayload) {
    		this.configPayload = configPayload;
    	}

    	@Override
    	public int getVersion() {
    		return 1;
    	}

    	@Override
    	public void write(DataOutputView out) throws IOException {
    		super.write(out);
    		out.writeUTF(configPayload);
    	}

    	@Override
    	public void read(DataInputView in) throws IOException {
    		super.read(in);
    		this.configPayload = in.readUTF();
    	}

    	@Override
    	public int hashCode() {
    		return getClass().hashCode();
    	}

    	@Override
    	public boolean equals(Object obj) {
    		return obj instanceof TestSerializerSnapshot;
    	}
    }
    */
}
