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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.TestLogger;
import org.junit.ClassRule;
import org.junit.Ignore;
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

import static org.apache.flink.runtime.state.filesystem.FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class PojoSerializerUpgradeTest extends TestLogger {

	@Parameterized.Parameters(name = "StateBackend: {0}")
	public static Collection<String> parameters () {
		return Arrays.asList(
			AbstractStateBackend.MEMORY_STATE_BACKEND_NAME,
			AbstractStateBackend.FS_STATE_BACKEND_NAME,
			AbstractStateBackend.ROCKSDB_STATE_BACKEND_NAME);
	}

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private StateBackend stateBackend;

	public PojoSerializerUpgradeTest(String backendType) throws IOException, DynamicCodeLoadingException {
		Configuration config = new Configuration();
		config.setString(CoreOptions.STATE_BACKEND, backendType);
		config.setString(CHECKPOINT_DIRECTORY_URI_CONF_KEY, temporaryFolder.newFolder().toURI().toString());
		stateBackend = AbstractStateBackend.loadStateBackendFromConfig(config, Thread.currentThread().getContextClassLoader(), null);
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
	 * We should be able to handle a changed field order
	 */
	@Test
	public void testChangedFieldOrder() throws Exception {
		testPojoSerializerUpgrade(SOURCE_A, SOURCE_B);
	}

	/**
	 * Changing field types should require a state migration
	 */
	@Test
	public void testChangedFieldTypes() throws Exception {
		assumeTrue("Running only for RocksDBStateBackend until FLINK-6804 has been fixed.", stateBackend instanceof RocksDBStateBackend);
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_C);
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
	 * Adding fields should require a state migration
	 */
	@Test
	public void testAdditionalField() throws Exception {
		assumeTrue("Running only for RocksDBStateBackend until FLINK-6804 has been fixed.", stateBackend instanceof RocksDBStateBackend);
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_D);
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
	 * Removing fields should require a state migration
	 */
	@Ignore("Ignore this test until FLINK-6801 has been fixed.")
	@Test
	public void testMissingField() throws Exception {
		try {
			testPojoSerializerUpgrade(SOURCE_A, SOURCE_E);
			fail("Expected a state migration exception.");
		} catch (Exception e) {
			if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
				// StateMigrationException expected
			} else {
				throw e;
			}
		}
	}

	public void testPojoSerializerUpgrade(String classSourceA, String classSourceB) throws Exception {
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

		OperatorStateHandles stateHandles = runOperator(
			taskConfiguration,
			executionConfig,
			new StreamMap<>(new StatefulMapper(true, false)),
			keySelector,
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
			new StreamMap<>(new StatefulMapper(true, true)),
			keySelector,
			stateBackend,
			classLoaderB,
			stateHandles,
			inputs);
	}

	private OperatorStateHandles runOperator(
			Configuration taskConfiguration,
			ExecutionConfig executionConfig,
			OneInputStreamOperator<Long, Long> operator,
			KeySelector<Long, Long> keySelector,
			StateBackend stateBackend,
			ClassLoader classLoader,
			OperatorStateHandles operatorStateHandles,
			Iterable<Long> input) throws Exception {

		final MockEnvironment environment = new MockEnvironment(
			"test task",
			32 * 1024,
			new MockInputSplitProvider(),
			256,
			taskConfiguration,
			executionConfig,
			16,
			1,
			0,
			classLoader);

		final KeyedOneInputStreamOperatorTestHarness<Long, Long, Long> harness = new KeyedOneInputStreamOperatorTestHarness<>(
			operator,
			keySelector,
			BasicTypeInfo.LONG_TYPE_INFO,
			environment);

		harness.setStateBackend(stateBackend);

		harness.setup();
		harness.initializeState(operatorStateHandles);
		harness.open();

		long timestamp = 0L;

		for (Long value : input) {
			harness.processElement(value, timestamp++);
		}


		long checkpointId = 1L;
		long checkpointTimestamp = timestamp + 1L;

		OperatorStateHandles stateHandles = harness.snapshot(checkpointId, checkpointTimestamp);

		harness.close();

		return stateHandles;
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

		private transient ValueState<Object> valueState;
		private transient MapState<Object, Object> mapState;
		private transient ListState<Object> listState;
		private transient ReducingState<Object> reducingState;
		private transient Class<?> pojoClass;
		private transient Field fieldA;
		private transient Field fieldB;

		public StatefulMapper(boolean keyed, boolean verify) {
			this.keyed = keyed;
			this.verify = verify;
		}

		@Override
		public Long map(Long value) throws Exception {
			Object pojo = pojoClass.newInstance();

			fieldA.set(pojo, value);
			fieldB.set(pojo, value + "");

			if (verify) {
				assertEquals(pojo, valueState.value());

				assertTrue(mapState.contains(pojo));
				assertEquals(pojo, mapState.get(pojo));

				Iterator<Object> listIterator = listState.get().iterator();

				boolean elementFound = false;

				while(listIterator.hasNext()) {
					elementFound |= pojo.equals(listIterator.next());
				}

				assertTrue(elementFound);

				assertEquals(pojo, reducingState.get());
			} else {
				valueState.update(pojo);
				mapState.put(pojo, pojo);
				listState.add(pojo);
				reducingState.add(pojo);
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
			fieldB = pojoClass.getDeclaredField("b");
			fieldA.setAccessible(true);
			fieldB.setAccessible(true);

			if (keyed) {
				valueState = context.getKeyedStateStore().getState(new ValueStateDescriptor<>("valueState", (Class<Object>) pojoClass));
				mapState = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>("mapState", (Class<Object>) pojoClass, (Class<Object>) pojoClass));
				listState = context.getKeyedStateStore().getListState(new ListStateDescriptor<>("listState", (Class<Object>) pojoClass));

				ReduceFunction<Object> reduceFunction = new FirstValueReducer<>();
				reducingState = context.getKeyedStateStore().getReducingState(new ReducingStateDescriptor<>("reducingState", reduceFunction, (Class<Object>) pojoClass));
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
