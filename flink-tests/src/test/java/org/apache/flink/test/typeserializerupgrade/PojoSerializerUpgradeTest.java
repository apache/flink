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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the state migration behaviour when the underlying POJO type changes
 * and one tries to recover from old state.
 */
@RunWith(Parameterized.class)
public class PojoSerializerUpgradeTest extends TestLogger {

	@Parameterized.Parameters(name = "StateBackend: {0}")
	public static Collection<String> parameters () {
		return Arrays.asList(
				StateBackendLoader.MEMORY_STATE_BACKEND_NAME,
				StateBackendLoader.FS_STATE_BACKEND_NAME,
				StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME);
	}

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private StateBackend stateBackend;

	public PojoSerializerUpgradeTest(String backendType) throws IOException, DynamicCodeLoadingException {
		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.STATE_BACKEND, backendType);
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		stateBackend = StateBackendLoader.loadStateBackendFromConfig(config, Thread.currentThread().getContextClassLoader(), null);
	}

	private static final String POJO_NAME = "Pojo";

	private static final String SOURCE_A =
		"import java.util.Objects;" +
		"public class Pojo { " +
		"private long a; " +
		"private String b; " +
		"public long getA() { return a;} " +
		"public void setA(long value) { a = value; }" +
		"public String getB() { return b; }" +
		"public void setB(String value) { b = value; }" +
		"@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b);} else { return false; }}" +
		"@Override public int hashCode() { return Objects.hash(a, b); } " +
		"@Override public String toString() {return \"(\" + a + \", \" + b + \")\";}}";

	// changed order of fields which should be recoverable
	private static final String SOURCE_B =
		"import java.util.Objects;" +
		"public class Pojo { " +
		"private String b; " +
		"private long a; " +
		"public long getA() { return a;} " +
		"public void setA(long value) { a = value; }" +
		"public String getB() { return b; }" +
		"public void setB(String value) { b = value; }" +
		"@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b);} else { return false; }}" +
		"@Override public int hashCode() { return Objects.hash(a, b); } " +
		"@Override public String toString() {return \"(\" + a + \", \" + b + \")\";}}";

	// changed type of a field which should not be recoverable
	private static final String SOURCE_C =
		"import java.util.Objects;" +
		"public class Pojo { " +
		"private double a; " +
		"private String b; " +
		"public double getA() { return a;} " +
		"public void setA(double value) { a = value; }" +
		"public String getB() { return b; }" +
		"public void setB(String value) { b = value; }" +
		"@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b);} else { return false; }}" +
		"@Override public int hashCode() { return Objects.hash(a, b); } " +
		"@Override public String toString() {return \"(\" + a + \", \" + b + \")\";}}";

	// additional field which should not be recoverable
	private static final String SOURCE_D =
		"import java.util.Objects;" +
		"public class Pojo { " +
		"private long a; " +
		"private String b; " +
		"private double c; " +
		"public long getA() { return a;} " +
		"public void setA(long value) { a = value; }" +
		"public String getB() { return b; }" +
		"public void setB(String value) { b = value; }" +
		"public double getC() { return c; } " +
		"public void setC(double value) { c = value; }" +
		"@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a && b.equals(other.b) && c == other.c;} else { return false; }}" +
		"@Override public int hashCode() { return Objects.hash(a, b, c); } " +
		"@Override public String toString() {return \"(\" + a + \", \" + b + \", \" + c + \")\";}}";

	// missing field which should not be recoverable
	private static final String SOURCE_E =
		"import java.util.Objects;" +
		"public class Pojo { " +
		"private long a; " +
		"public long getA() { return a;} " +
		"public void setA(long value) { a = value; }" +
		"@Override public boolean equals(Object obj) { if (obj instanceof Pojo) { Pojo other = (Pojo) obj; return a == other.a;} else { return false; }}" +
		"@Override public int hashCode() { return Objects.hash(a); } " +
		"@Override public String toString() {return \"(\" + a + \")\";}}";

	/**
	 * We should be able to handle a changed field order of a POJO as keyed state.
	 */
	@Test
	public void testChangedFieldOrderWithKeyedState() throws Exception {
		testPojoSerializerUpgrade(SOURCE_A, SOURCE_B, true, true);
	}

	/**
	 * We should be able to handle a changed field order of a POJO as operator state.
	 */
	@Test
	public void testChangedFieldOrderWithOperatorState() throws Exception {
		testPojoSerializerUpgrade(SOURCE_A, SOURCE_B, true, false);
	}

	/**
	 * Changing field types of a POJO as keyed state should require a state migration.
	 */
	@Test
	public void testChangedFieldTypesWithKeyedState() throws Exception {
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_C, true, true);
			fail("Expected a state migration exception.");
		} catch (Exception e) {
			if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
				// StateMigrationException expected
			} else {
				throw e;
			}
		}
	}

	/**
	 * Changing field types of a POJO as operator state should require a state migration.
	 */
	@Test
	public void testChangedFieldTypesWithOperatorState() throws Exception {
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_C, true, false);
			fail("Expected a state migration exception.");
		} catch (Exception e) {
			if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
				// StateMigrationException expected
			} else {
				throw e;
			}
		}
	}

	/**
	 * Adding fields to a POJO as keyed state should require a state migration.
	 */
	@Test
	public void testAdditionalFieldWithKeyedState() throws Exception {
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_D, true, true);
			fail("Expected a state migration exception.");
		} catch (Exception e) {
			if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
				// StateMigrationException expected
			} else {
				throw e;
			}
		}
	}

	/**
	 * Adding fields to a POJO as operator state should require a state migration.
	 */
	@Test
	public void testAdditionalFieldWithOperatorState() throws Exception {
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_D, true, false);
			fail("Expected a state migration exception.");
		} catch (Exception e) {
			if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
				// StateMigrationException expected
			} else {
				throw e;
			}
		}
	}

	/**
	 * Removing fields from a POJO as keyed state should require a state migration.
	 */
	@Test
	public void testMissingFieldWithKeyedState() throws Exception {
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_E, false, true);
			fail("Expected a state migration exception.");
		} catch (Exception e) {
			if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
				// StateMigrationException expected
			} else {
				throw e;
			}
		}
	}

	/**
	 * Removing fields from a POJO as operator state should require a state migration.
	 */
	@Test
	public void testMissingFieldWithOperatorState() throws Exception {
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_E, false, false);
			fail("Expected a state migration exception.");
		} catch (Exception e) {
			if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
				// StateMigrationException expected
			} else {
				throw e;
			}
		}
	}

	public void testPojoSerializerUpgrade(String classSourceA, String classSourceB, boolean hasBField, boolean isKeyedState) throws Exception {
		final Configuration taskConfiguration = new Configuration();
		final ExecutionConfig executionConfig = new ExecutionConfig();
		final KeySelector<Long, Long> keySelector = new IdentityKeySelector<>();
		final Collection<Long> inputs = Arrays.asList(1L, 2L, 45L, 67L, 1337L);

		// run the program with classSourceA
		File rootPath = temporaryFolder.newFolder();
		File sourceFile = writeSourceFile(rootPath, POJO_NAME + ".java", classSourceA);
		compileClass(sourceFile);

		final ClassLoader classLoader = URLClassLoader.newInstance(
			new URL[]{rootPath.toURI().toURL()},
			Thread.currentThread().getContextClassLoader());

		OperatorSubtaskState stateHandles = runOperator(
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
		rootPath = temporaryFolder.newFolder();

		sourceFile = writeSourceFile(rootPath, POJO_NAME + ".java", classSourceB);
		compileClass(sourceFile);

		final ClassLoader classLoaderB = URLClassLoader.newInstance(
			new URL[]{rootPath.toURI().toURL()},
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
			Iterable<Long> input) throws Exception {

		try (final MockEnvironment environment = new MockEnvironment(
			"test task",
			32 * 1024,
			new MockInputSplitProvider(),
			256,
			taskConfiguration,
			executionConfig,
			new TestTaskStateManager(),
			16,
			1,
			0,
			classLoader)) {

			OneInputStreamOperatorTestHarness<Long, Long> harness = null;
			try {
				if (isKeyedState) {
					harness = new KeyedOneInputStreamOperatorTestHarness<>(
						operator,
						keySelector,
						BasicTypeInfo.LONG_TYPE_INFO,
						environment);
				} else {
					harness = new OneInputStreamOperatorTestHarness<>(operator, LongSerializer.INSTANCE, environment);
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
		return compiler.run(null, null, null, sourceFile.getPath());
	}

	private static final class StatefulMapper extends RichMapFunction<Long, Long> implements CheckpointedFunction {

		private static final long serialVersionUID = -520490739059396832L;

		private final boolean keyed;
		private final boolean verify;
		private final boolean hasBField;

		// keyed states
		private transient ValueState<Object> keyedValueState;
		private transient MapState<Object, Object> keyedMapState;
		private transient ListState<Object> keyedListState;
		private transient ReducingState<Object> keyedReducingState;

		// operator states
		private transient ListState<Object> partitionableListState;
		private transient ListState<Object> unionListState;

		private transient Class<?> pojoClass;
		private transient Field fieldA;
		private transient Field fieldB;

		public StatefulMapper(boolean keyed, boolean verify, boolean hasBField) {
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
					assertEquals(pojo, keyedValueState.value());

					assertTrue(keyedMapState.contains(pojo));
					assertEquals(pojo, keyedMapState.get(pojo));

					Iterator<Object> listIterator = keyedListState.get().iterator();

					boolean elementFound = false;

					while (listIterator.hasNext()) {
						elementFound |= pojo.equals(listIterator.next());
					}

					assertTrue(elementFound);

					assertEquals(pojo, keyedReducingState.get());
				} else {
					boolean elementFound = false;
					Iterator<Object> listIterator = partitionableListState.get().iterator();
					while (listIterator.hasNext()) {
						elementFound |= pojo.equals(listIterator.next());
					}
					assertTrue(elementFound);

					elementFound = false;
					listIterator = unionListState.get().iterator();
					while (listIterator.hasNext()) {
						elementFound |= pojo.equals(listIterator.next());
					}
					assertTrue(elementFound);
				}
			} else {
				if (keyed) {
					keyedValueState.update(pojo);
					keyedMapState.put(pojo, pojo);
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
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

		}

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
				keyedValueState = context.getKeyedStateStore().getState(
					new ValueStateDescriptor<>("keyedValueState", (Class<Object>) pojoClass));
				keyedMapState = context.getKeyedStateStore().getMapState(
					new MapStateDescriptor<>("keyedMapState", (Class<Object>) pojoClass, (Class<Object>) pojoClass));
				keyedListState = context.getKeyedStateStore().getListState(
					new ListStateDescriptor<>("keyedListState", (Class<Object>) pojoClass));

				ReduceFunction<Object> reduceFunction = new FirstValueReducer<>();
				keyedReducingState = context.getKeyedStateStore().getReducingState(
					new ReducingStateDescriptor<>("keyedReducingState", reduceFunction, (Class<Object>) pojoClass));
			} else {
				partitionableListState = context.getOperatorStateStore().getListState(
					new ListStateDescriptor<>("partitionableListState", (Class<Object>) pojoClass));
				unionListState = context.getOperatorStateStore().getUnionListState(
					new ListStateDescriptor<>("unionListState", (Class<Object>) pojoClass));
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
