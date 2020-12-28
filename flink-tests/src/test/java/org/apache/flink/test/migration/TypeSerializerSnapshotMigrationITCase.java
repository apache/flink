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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.checkpointing.utils.MigrationTestUtils;
import org.apache.flink.test.checkpointing.utils.SavepointMigrationTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Migration IT cases for upgrading a legacy {@link TypeSerializerConfigSnapshot} that is written in
 * checkpoints to {@link TypeSerializerSnapshot} interface.
 *
 * <p>The savepoints used by this test were written with a serializer snapshot class that extends
 * {@link TypeSerializerConfigSnapshot}, as can be seen in the commented out code at the end of this
 * class. On restore, we change the snapshot to implement directly a {@link TypeSerializerSnapshot}.
 */
@RunWith(Parameterized.class)
public class TypeSerializerSnapshotMigrationITCase extends SavepointMigrationTestBase {

    private static final int NUM_SOURCE_ELEMENTS = 4;

    /**
     * This test runs in either of two modes: 1) we want to generate the binary savepoint, i.e. we
     * have to run the checkpointing functions 2) we want to verify restoring, so we have to run the
     * checking functions.
     */
    public enum ExecutionMode {
        PERFORM_SAVEPOINT,
        VERIFY_SAVEPOINT
    }

    // TODO change this to PERFORM_SAVEPOINT to regenerate binary savepoints
    // TODO Note: You should generate the savepoint based on the release branch instead of the
    // master.
    private final ExecutionMode executionMode = ExecutionMode.VERIFY_SAVEPOINT;

    @Parameterized.Parameters(name = "Migrate Savepoint / Backend: {0}")
    public static Collection<Tuple2<MigrationVersion, String>> parameters() {
        return Arrays.asList(
                Tuple2.of(MigrationVersion.v1_3, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_3, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_4, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_4, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_6, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_6, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_7, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_7, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_8, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_8, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_9, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_9, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_10, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_10, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_11, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
                Tuple2.of(MigrationVersion.v1_11, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME));
    }

    private final MigrationVersion testMigrateVersion;
    private final String testStateBackend;

    public TypeSerializerSnapshotMigrationITCase(
            Tuple2<MigrationVersion, String> testMigrateVersionAndBackend) throws Exception {
        this.testMigrateVersion = testMigrateVersionAndBackend.f0;
        this.testStateBackend = testMigrateVersionAndBackend.f1;
    }

    @Test
    public void testSavepoint() throws Exception {
        final int parallelism = 1;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        switch (testStateBackend) {
            case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
                env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));
                break;
            case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
                env.setStateBackend(new MemoryStateBackend());
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

        if (executionMode == ExecutionMode.PERFORM_SAVEPOINT) {
            executeAndSavepoint(
                    env,
                    "src/test/resources/" + getSavepointPath(testMigrateVersion, testStateBackend),
                    new Tuple2<>(
                            MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS));
        } else {
            restoreAndExecute(
                    env,
                    getResourceFilename(getSavepointPath(testMigrateVersion, testStateBackend)),
                    new Tuple2<>(
                            MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS));
        }
    }

    private String getSavepointPath(MigrationVersion savepointVersion, String backendType) {
        switch (backendType) {
            case "rocksdb":
                return "type-serializer-snapshot-migration-itcase-flink"
                        + savepointVersion
                        + "-rocksdb-savepoint";
            case "jobmanager":
                return "type-serializer-snapshot-migration-itcase-flink"
                        + savepointVersion
                        + "-savepoint";
            default:
                throw new UnsupportedOperationException();
        }
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
