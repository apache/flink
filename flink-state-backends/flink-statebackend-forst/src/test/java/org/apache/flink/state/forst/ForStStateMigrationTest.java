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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.v2.AbstractKeyedState;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.state.forst.ForStTestUtils.createKeyedStateBackend;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/** Tests for the State Migration of {@link ForStKeyedStateBackend}. */
public class ForStStateMigrationTest extends ForStStateTestBase {

    @Test
    void testFortMapStateKeySchemaChanged() throws Exception {
        MapStateDescriptor<Integer, String> descriptorV1 =
                new MapStateDescriptor<>(
                        "testState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);

        MapStateDescriptor<String, String> descriptorV2 =
                new MapStateDescriptor<>(
                        "testState", StringSerializer.INSTANCE, StringSerializer.INSTANCE);

        MapState<Integer, String> mapState =
                keyedBackend.createState(1, IntSerializer.INSTANCE, descriptorV1);
        setCurrentContext("test", "test");
        for (int i = 0; i < 10; i++) {
            mapState.asyncPut(i, String.valueOf(i));
        }
        drain();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                keyedBackend.snapshot(
                        1L,
                        System.currentTimeMillis(),
                        env.getCheckpointStorageAccess()
                                .resolveCheckpointStorageLocation(
                                        1L, CheckpointStorageLocationReference.getDefault()),
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        if (!snapshot.isDone()) {
            snapshot.run();
        }
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
        KeyedStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();
        IOUtils.closeQuietly(keyedBackend);
        keyedBackend.dispose();

        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        ForStStateBackend forStStateBackend =
                new ForStStateBackend().configure(configuration, null);
        keyedBackend =
                createKeyedStateBackend(
                        forStStateBackend,
                        env,
                        StringSerializer.INSTANCE,
                        Collections.singletonList(stateHandle));
        keyedBackend.setup(aec);
        try {
            keyedBackend.createState(1, IntSerializer.INSTANCE, descriptorV2);
            fail("Expected a state migration exception.");
        } catch (Exception e) {
            if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
                // StateMigrationException expected
            } else {
                throw e;
            }
        }
    }

    @Test
    void testKryoRestoreResilienceWithDifferentRegistrationOrder(@TempDir File newTmpDir)
            throws Exception {

        // register A first then B
        ((SerializerConfigImpl) env.getExecutionConfig().getSerializerConfig())
                .registerKryoType(StateBackendTestBase.TestNestedPojoClassA.class);
        ((SerializerConfigImpl) env.getExecutionConfig().getSerializerConfig())
                .registerKryoType(StateBackendTestBase.TestNestedPojoClassB.class);

        TypeInformation<StateBackendTestBase.TestPojo> pojoType =
                new GenericTypeInfo<>(StateBackendTestBase.TestPojo.class);

        // make sure that we are in fact using the KryoSerializer
        assertThat(pojoType.createSerializer(env.getExecutionConfig().getSerializerConfig()))
                .isInstanceOf(KryoSerializer.class);

        ValueStateDescriptor<StateBackendTestBase.TestPojo> stateDescriptor =
                new ValueStateDescriptor<>("id", pojoType);

        ValueState<StateBackendTestBase.TestPojo> state =
                keyedBackend.getOrCreateKeyedState(1, IntSerializer.INSTANCE, stateDescriptor);

        // access the internal state representation to retrieve the original Kryo registration
        // ids;
        // these will be later used to check that on restore, the new Kryo serializer has
        // reconfigured itself to
        // have identical mappings
        AbstractKeyedState abstractKeyedState = (AbstractKeyedState) state;
        KryoSerializer<StateBackendTestBase.TestPojo> kryoSerializer =
                (KryoSerializer<StateBackendTestBase.TestPojo>)
                        abstractKeyedState.getValueSerializer();
        int mainPojoClassRegistrationId =
                kryoSerializer
                        .getKryo()
                        .getRegistration(StateBackendTestBase.TestPojo.class)
                        .getId();
        int nestedPojoClassARegistrationId =
                kryoSerializer
                        .getKryo()
                        .getRegistration(StateBackendTestBase.TestNestedPojoClassA.class)
                        .getId();
        int nestedPojoClassBRegistrationId =
                kryoSerializer
                        .getKryo()
                        .getRegistration(StateBackendTestBase.TestNestedPojoClassB.class)
                        .getId();

        // ============== create snapshot of current configuration ==============

        // make some more modifications
        setCurrentContext("test", "test");
        state.asyncUpdate(
                new StateBackendTestBase.TestPojo(
                        "u1",
                        1,
                        new StateBackendTestBase.TestNestedPojoClassA(1.0, 2),
                        new StateBackendTestBase.TestNestedPojoClassB(2.3, "foo")));

        drain();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                keyedBackend.snapshot(
                        1L,
                        System.currentTimeMillis(),
                        env.getCheckpointStorageAccess()
                                .resolveCheckpointStorageLocation(
                                        1L, CheckpointStorageLocationReference.getDefault()),
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        if (!snapshot.isDone()) {
            snapshot.run();
        }
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
        KeyedStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();
        IOUtils.closeQuietly(keyedBackend);
        keyedBackend.dispose();

        env = getMockEnvironment(newTmpDir);
        ((SerializerConfigImpl) env.getExecutionConfig().getSerializerConfig())
                .registerKryoType(
                        StateBackendTestBase.TestNestedPojoClassB
                                .class); // this time register B first
        ((SerializerConfigImpl) env.getExecutionConfig().getSerializerConfig())
                .registerKryoType(StateBackendTestBase.TestNestedPojoClassA.class);

        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        ForStStateBackend forStStateBackend =
                new ForStStateBackend().configure(configuration, null);
        ForStKeyedStateBackend<String> restoredKeyedStateBackend =
                createKeyedStateBackend(
                        forStStateBackend,
                        env,
                        StringSerializer.INSTANCE,
                        Collections.singletonList(stateHandle));
        restoredKeyedStateBackend.setup(aec);

        // re-initialize to ensure that we create the KryoSerializer from scratch, otherwise
        // initializeSerializerUnlessSet would not pick up our new config
        stateDescriptor = new ValueStateDescriptor<>("id", pojoType);
        state =
                restoredKeyedStateBackend.getOrCreateKeyedState(
                        1, IntSerializer.INSTANCE, stateDescriptor);

        // verify that on restore, the serializer that the state handle uses has reconfigured
        // itself to have
        // identical Kryo registration ids compared to the previous execution
        abstractKeyedState = (AbstractKeyedState) state;
        kryoSerializer =
                (KryoSerializer<StateBackendTestBase.TestPojo>)
                        abstractKeyedState.getValueSerializer();
        assertThat(
                        kryoSerializer
                                .getKryo()
                                .getRegistration(StateBackendTestBase.TestPojo.class)
                                .getId())
                .isEqualTo(mainPojoClassRegistrationId);
        assertThat(
                        kryoSerializer
                                .getKryo()
                                .getRegistration(StateBackendTestBase.TestNestedPojoClassA.class)
                                .getId())
                .isEqualTo(nestedPojoClassARegistrationId);
        assertThat(
                        kryoSerializer
                                .getKryo()
                                .getRegistration(StateBackendTestBase.TestNestedPojoClassB.class)
                                .getId())
                .isEqualTo(nestedPojoClassBRegistrationId);

        setCurrentContext("test", "test");

        // update to test state backends that eagerly serialize, such as RocksDB
        state.asyncUpdate(
                new StateBackendTestBase.TestPojo(
                        "u1",
                        11,
                        new StateBackendTestBase.TestNestedPojoClassA(22.1, 12),
                        new StateBackendTestBase.TestNestedPojoClassB(1.23, "foobar")));

        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot1 =
                restoredKeyedStateBackend.snapshot(
                        1L,
                        System.currentTimeMillis(),
                        env.getCheckpointStorageAccess()
                                .resolveCheckpointStorageLocation(
                                        1L, CheckpointStorageLocationReference.getDefault()),
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        if (!snapshot1.isDone()) {
            snapshot1.run();
        }
        snapshot1.get().discardState();
    }

    @Test
    void testStateNamespaceSerializerChanged() throws Exception {
        MapStateDescriptor<Integer, String> descriptor =
                new MapStateDescriptor<>(
                        "testState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);

        // set the old namespace serializer to IntSerializer.INSTANCE
        MapState<Integer, String> mapState =
                keyedBackend.createState(1, IntSerializer.INSTANCE, descriptor);

        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                keyedBackend.snapshot(
                        1L,
                        System.currentTimeMillis(),
                        env.getCheckpointStorageAccess()
                                .resolveCheckpointStorageLocation(
                                        1L, CheckpointStorageLocationReference.getDefault()),
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        if (!snapshot.isDone()) {
            snapshot.run();
        }
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
        KeyedStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();
        IOUtils.closeQuietly(keyedBackend);
        keyedBackend.dispose();

        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        ForStStateBackend forStStateBackend =
                new ForStStateBackend().configure(configuration, null);
        keyedBackend =
                createKeyedStateBackend(
                        forStStateBackend,
                        env,
                        StringSerializer.INSTANCE,
                        Collections.singletonList(stateHandle));
        keyedBackend.setup(aec);
        try {

            // change new NSSerializer to StringSerializer
            keyedBackend.createState("String", StringSerializer.INSTANCE, descriptor);
            fail("Expected a state migration exception.");
        } catch (Exception e) {
            if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
                // StateMigrationException expected
            } else {
                throw e;
            }
        }
    }
}
