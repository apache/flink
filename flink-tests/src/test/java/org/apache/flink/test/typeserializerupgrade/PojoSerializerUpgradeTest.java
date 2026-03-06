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

package org.apache.flink.test.typeserializerupgrade;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the state migration behaviour when the underlying POJO type changes and one tries to
 * recover from old state.
 */
@ExtendWith({TestLoggerExtension.class, ParameterizedTestExtension.class})
class PojoSerializerUpgradeTest {

    @Parameter private String backendType;

    @Parameters(name = "StateBackend: {0}")
    public static Collection<String> parameters() {
        return Arrays.asList(
                StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
                StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME);
    }

    @TempDir private Path temporaryFolder;

    private StateBackend stateBackend;

    @BeforeEach
    void before() throws DynamicCodeLoadingException, IOException {
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, backendType);
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.toUri().toString());
        stateBackend =
                StateBackendLoader.loadStateBackendFromConfig(
                        config, Thread.currentThread().getContextClassLoader(), null);
    }

    private static final String POJO_NAME = "Pojo";

    private static final String SOURCE_A =
            "import java.util.Objects;"
                    + "public class Pojo { "
                    + "private long a; "
                    + "private String b; "
                    + "public long getA() { return a;} "
                    + "public void setA(long value) { a = value; }"
                    + "public String getB() { return b; }"
                    + "public void setB(String value) { b = value; }"
                    + "@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b);} else { return false; }}"
                    + "@Override public int hashCode() { return Objects.hash(a, b); } "
                    + "@Override public String toString() {return \"(\" + a + \", \" + b + \")\";}}";

    // changed order of fields which should be recoverable
    private static final String SOURCE_B =
            "import java.util.Objects;"
                    + "public class Pojo { "
                    + "private String b; "
                    + "private long a; "
                    + "public long getA() { return a;} "
                    + "public void setA(long value) { a = value; }"
                    + "public String getB() { return b; }"
                    + "public void setB(String value) { b = value; }"
                    + "@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b);} else { return false; }}"
                    + "@Override public int hashCode() { return Objects.hash(a, b); } "
                    + "@Override public String toString() {return \"(\" + a + \", \" + b + \")\";}}";

    // changed type of a field which should not be recoverable
    private static final String SOURCE_C =
            "import java.util.Objects;"
                    + "public class Pojo { "
                    + "private double a; "
                    + "private String b; "
                    + "public double getA() { return a;} "
                    + "public void setA(double value) { a = value; }"
                    + "public String getB() { return b; }"
                    + "public void setB(String value) { b = value; }"
                    + "@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b);} else { return false; }}"
                    + "@Override public int hashCode() { return Objects.hash(a, b); } "
                    + "@Override public String toString() {return \"(\" + a + \", \" + b + \")\";}}";

    // additional field which should not be recoverable
    private static final String SOURCE_D =
            "import java.util.Objects;"
                    + "public class Pojo { "
                    + "private long a; "
                    + "private String b; "
                    + "private double c; "
                    + "public long getA() { return a;} "
                    + "public void setA(long value) { a = value; }"
                    + "public String getB() { return b; }"
                    + "public void setB(String value) { b = value; }"
                    + "public double getC() { return c; } "
                    + "public void setC(double value) { c = value; }"
                    + "@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b) && c == other.c;} else { return false; }}"
                    + "@Override public int hashCode() { return Objects.hash(a, b, c); } "
                    + "@Override public String toString() {return \"(\" + a + \", \" + b + \", \" + c + \")\";}}";

    // missing field which should not be recoverable
    private static final String SOURCE_E =
            "import java.util.Objects;"
                    + "public class Pojo { "
                    + "private long a; "
                    + "public long getA() { return a;} "
                    + "public void setA(long value) { a = value; }"
                    + "@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a;} else { return false; }}"
                    + "@Override public int hashCode() { return Objects.hash(a); } "
                    + "@Override public String toString() {return \"(\" + a + \")\";}}";

    /** We should be able to handle a changed field order of a POJO as keyed state. */
    @TestTemplate
    void testChangedFieldOrderWithKeyedState() throws Exception {
        testPojoSerializerUpgrade(SOURCE_A, SOURCE_B, true, true);
    }

    /** We should be able to handle a changed field order of a POJO as operator state. */
    @TestTemplate
    void testChangedFieldOrderWithOperatorState() throws Exception {
        testPojoSerializerUpgrade(SOURCE_A, SOURCE_B, true, false);
    }

    /** Changing field types of a POJO as keyed state should require a state migration. */
    @TestTemplate
    void testChangedFieldTypesWithKeyedState() {
        assertThatThrownBy(() -> testPojoSerializerUpgrade(SOURCE_A, SOURCE_C, true, true))
                .hasRootCauseInstanceOf(StateMigrationException.class);
    }

    /** Changing field types of a POJO as operator state should require a state migration. */
    @TestTemplate
    void testChangedFieldTypesWithOperatorState() {
        assertThatThrownBy(() -> testPojoSerializerUpgrade(SOURCE_A, SOURCE_C, true, false))
                .isInstanceOf(StateMigrationException.class);
    }

    /** Adding fields to a POJO as keyed state should succeed. */
    @TestTemplate
    void testAdditionalFieldWithKeyedState() throws Exception {
        testPojoSerializerUpgrade(SOURCE_A, SOURCE_D, true, true);
    }

    /** Adding fields to a POJO as operator state should succeed. */
    @TestTemplate
    void testAdditionalFieldWithOperatorState() throws Exception {
        testPojoSerializerUpgrade(SOURCE_A, SOURCE_D, true, false);
    }

    /** Removing fields from a POJO as keyed state should succeed. */
    @TestTemplate
    void testMissingFieldWithKeyedState() throws Exception {
        testPojoSerializerUpgrade(SOURCE_A, SOURCE_E, false, true);
    }

    /** Removing fields from a POJO as operator state should succeed. */
    @TestTemplate
    void testMissingFieldWithOperatorState() throws Exception {
        testPojoSerializerUpgrade(SOURCE_A, SOURCE_E, false, false);
    }

    private void testPojoSerializerUpgrade(
            String classSourceA, String classSourceB, boolean hasBField, boolean isKeyedState)
            throws Exception {
        final Configuration taskConfiguration = new Configuration();
        final ExecutionConfig executionConfig = new ExecutionConfig();
        final KeySelector<Long, Long> keySelector = new IdentityKeySelector<>();
        final Collection<Long> inputs = Arrays.asList(1L, 2L, 45L, 67L, 1337L);

        // run the program with classSourceA
        File rootPath = TempDirUtils.newFolder(temporaryFolder);
        File sourceFile = writeSourceFile(rootPath, POJO_NAME + ".java", classSourceA);
        compileClass(sourceFile);

        final ClassLoader classLoader =
                URLClassLoader.newInstance(
                        new URL[] {rootPath.toURI().toURL()},
                        Thread.currentThread().getContextClassLoader());

        OperatorSubtaskState stateHandles =
                runOperator(
                        taskConfiguration,
                        executionConfig,
                        new StreamMap<>(new StatefulMapper(isKeyedState, false, hasBField)),
                        keySelector,
                        isKeyedState,
                        stateBackend,
                        classLoader,
                        null,
                        inputs);

        // run the program with classSourceB
        rootPath = TempDirUtils.newFolder(temporaryFolder);

        sourceFile = writeSourceFile(rootPath, POJO_NAME + ".java", classSourceB);
        compileClass(sourceFile);

        final ClassLoader classLoaderB =
                URLClassLoader.newInstance(
                        new URL[] {rootPath.toURI().toURL()},
                        Thread.currentThread().getContextClassLoader());

        runOperator(
                taskConfiguration,
                executionConfig,
                new StreamMap<>(new StatefulMapper(isKeyedState, true, hasBField)),
                keySelector,
                isKeyedState,
                stateBackend,
                classLoaderB,
                stateHandles,
                inputs);
    }

    private OperatorSubtaskState runOperator(
            Configuration taskConfiguration,
            ExecutionConfig executionConfig,
            OneInputStreamOperator<Long, Long> operator,
            KeySelector<Long, Long> keySelector,
            boolean isKeyedState,
            StateBackend stateBackend,
            ClassLoader classLoader,
            OperatorSubtaskState operatorSubtaskState,
            Iterable<Long> input)
            throws Exception {

        try (final MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("test task")
                        .setManagedMemorySize(32 * 1024)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(256)
                        .setTaskConfiguration(taskConfiguration)
                        .setExecutionConfig(executionConfig)
                        .setMaxParallelism(16)
                        .setUserCodeClassLoader(classLoader)
                        .build()) {

            OneInputStreamOperatorTestHarness<Long, Long> harness = null;
            try {
                if (isKeyedState) {
                    harness =
                            new KeyedOneInputStreamOperatorTestHarness<>(
                                    operator,
                                    keySelector,
                                    BasicTypeInfo.LONG_TYPE_INFO,
                                    environment);
                } else {
                    harness =
                            new OneInputStreamOperatorTestHarness<>(
                                    operator, LongSerializer.INSTANCE, environment);
                }

                harness.setStateBackend(stateBackend);

                harness.setup();
                harness.initializeState(operatorSubtaskState);
                harness.open();

                long timestamp = 0L;

                for (Long value : input) {
                    harness.processElement(value, timestamp++);
                }

                long checkpointId = 1L;
                long checkpointTimestamp = timestamp + 1L;

                return harness.snapshot(checkpointId, checkpointTimestamp);
            } finally {
                IOUtils.closeQuietly(harness);
            }
        }
    }

    private static File writeSourceFile(File root, String name, String source) throws IOException {
        File sourceFile = new File(root, name);

        sourceFile.getParentFile().mkdirs();

        try (FileWriter writer = new FileWriter(sourceFile)) {
            writer.write(source);
        }

        return sourceFile;
    }

    private static int compileClass(File sourceFile) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        return compiler.run(null, null, null, "-proc:none", sourceFile.getPath());
    }

    private static final class StatefulMapper extends RichMapFunction<Long, Long>
            implements CheckpointedFunction {

        private static final long serialVersionUID = -520490739059396832L;

        private final boolean keyed;
        private final boolean verify;
        private final boolean hasBField;

        // keyed states
        private transient ValueState<Object> keyedValueState;
        private transient ListState<Object> keyedListState;
        private transient ReducingState<Object> keyedReducingState;

        // operator states
        private transient ListState<Object> partitionableListState;
        private transient ListState<Object> unionListState;

        private transient Class<?> pojoClass;
        private transient Field fieldA;
        private transient Field fieldB;

        StatefulMapper(boolean keyed, boolean verify, boolean hasBField) {
            this.keyed = keyed;
            this.verify = verify;
            this.hasBField = hasBField;
        }

        @Override
        public Long map(Long value) throws Exception {
            Object pojo = pojoClass.newInstance();

            fieldA.set(pojo, value);

            if (hasBField) {
                fieldB.set(pojo, value + "");
            }

            if (verify) {
                if (keyed) {
                    assertThat(keyedValueState.value()).isEqualTo(pojo);

                    Iterator<Object> listIterator = keyedListState.get().iterator();

                    boolean elementFound = false;

                    while (listIterator.hasNext()) {
                        elementFound |= pojo.equals(listIterator.next());
                    }

                    assertThat(elementFound).isTrue();

                    assertThat(keyedReducingState.get()).isEqualTo(pojo);
                } else {
                    boolean elementFound = false;
                    Iterator<Object> listIterator = partitionableListState.get().iterator();
                    while (listIterator.hasNext()) {
                        elementFound |= pojo.equals(listIterator.next());
                    }
                    assertThat(elementFound).isTrue();

                    elementFound = false;
                    listIterator = unionListState.get().iterator();
                    while (listIterator.hasNext()) {
                        elementFound |= pojo.equals(listIterator.next());
                    }
                    assertThat(elementFound).isTrue();
                }
            } else {
                if (keyed) {
                    keyedValueState.update(pojo);
                    keyedListState.add(pojo);
                    keyedReducingState.add(pojo);
                } else {
                    partitionableListState.add(pojo);
                    unionListState.add(pojo);
                }
            }

            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @SuppressWarnings("unchecked")
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            pojoClass = getRuntimeContext().getUserCodeClassLoader().loadClass(POJO_NAME);

            fieldA = pojoClass.getDeclaredField("a");
            fieldA.setAccessible(true);

            if (hasBField) {
                fieldB = pojoClass.getDeclaredField("b");
                fieldB.setAccessible(true);
            }

            if (keyed) {
                keyedValueState =
                        context.getKeyedStateStore()
                                .getState(
                                        new ValueStateDescriptor<>(
                                                "keyedValueState", (Class<Object>) pojoClass));
                keyedListState =
                        context.getKeyedStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "keyedListState", (Class<Object>) pojoClass));

                ReduceFunction<Object> reduceFunction = new FirstValueReducer<>();
                keyedReducingState =
                        context.getKeyedStateStore()
                                .getReducingState(
                                        new ReducingStateDescriptor<>(
                                                "keyedReducingState",
                                                reduceFunction,
                                                (Class<Object>) pojoClass));
            } else {
                partitionableListState =
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "partitionableListState",
                                                (Class<Object>) pojoClass));
                unionListState =
                        context.getOperatorStateStore()
                                .getUnionListState(
                                        new ListStateDescriptor<>(
                                                "unionListState", (Class<Object>) pojoClass));
            }
        }
    }

    private static final class FirstValueReducer<T> implements ReduceFunction<T> {

        private static final long serialVersionUID = -9222976423336835926L;

        @Override
        public T reduce(T value1, T value2) throws Exception {
            return value1;
        }
    }

    private static final class IdentityKeySelector<T> implements KeySelector<T, T> {

        private static final long serialVersionUID = -3263628393881929147L;

        @Override
        public T getKey(T value) throws Exception {
            return value;
        }
    }
}
