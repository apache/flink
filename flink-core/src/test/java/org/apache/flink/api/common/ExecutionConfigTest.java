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

package org.apache.flink.api.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExecutionConfigTest extends TestLogger {

	@Test
	public void testDoubleTypeRegistration() {
		ExecutionConfig config = new ExecutionConfig();
		List<Class<?>> types = Arrays.<Class<?>>asList(Double.class, Integer.class, Double.class);
		List<Class<?>> expectedTypes = Arrays.<Class<?>>asList(Double.class, Integer.class);

		for (Class<?> tpe: types) {
			config.registerKryoType(tpe);
		}

		int counter = 0;

		for (Class<?> tpe: config.getRegisteredKryoTypes()){
			assertEquals(tpe, expectedTypes.get(counter++));
		}

		assertEquals(expectedTypes.size(), counter);
	}

	@Test
	public void testConfigurationOfParallelism() {
		ExecutionConfig config = new ExecutionConfig();

		// verify explicit change in parallelism
		int parallelism = 36;
		config.setParallelism(parallelism);

		assertEquals(parallelism, config.getParallelism());

		// verify that parallelism is reset to default flag value
		parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
		config.setParallelism(parallelism);

		assertEquals(parallelism, config.getParallelism());
	}

	@Test
	public void testDisableGenericTypes() {
		ExecutionConfig conf = new ExecutionConfig();
		TypeInformation<Object> typeInfo = new GenericTypeInfo<Object>(Object.class);

		// by default, generic types are supported
		TypeSerializer<Object> serializer = typeInfo.createSerializer(conf);
		assertTrue(serializer instanceof KryoSerializer);

		// expect an exception when generic types are disabled
		conf.disableGenericTypes();
		try {
			typeInfo.createSerializer(conf);
			fail("should have failed with an exception");
		}
		catch (UnsupportedOperationException e) {
			// expected
		}
	}

	@Test
	public void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
		final Random r = new Random();

		final int parallelism = 1 + r.nextInt(10);
		final boolean closureCleanerEnabled = r.nextBoolean(), 
				forceAvroEnabled = r.nextBoolean(),
				forceKryoEnabled = r.nextBoolean(),
				disableGenericTypes = r.nextBoolean(),
				objectReuseEnabled = r.nextBoolean();

		final ExecutionConfig config = new ExecutionConfig();

		if (closureCleanerEnabled) {
			config.enableClosureCleaner();
		} else {
			config.disableClosureCleaner();
		}
		if (forceAvroEnabled) {
			config.enableForceAvro();
		} else {
			config.disableForceAvro();
		}
		if (forceKryoEnabled) {
			config.enableForceKryo();
		} else {
			config.disableForceKryo();
		}
		if (disableGenericTypes) {
			config.disableGenericTypes();
		} else {
			config.enableGenericTypes();
		}
		if (objectReuseEnabled) {
			config.enableObjectReuse();
		} else {
			config.disableObjectReuse();
		}
		config.setParallelism(parallelism);

		final ExecutionConfig copy1 = CommonTestUtils.createCopySerializable(config);
		final ExecutionConfig copy2 = new SerializedValue<>(config).deserializeValue(getClass().getClassLoader());

		assertNotNull(copy1);
		assertNotNull(copy2);

		assertEquals(config, copy1);
		assertEquals(config, copy2);

		assertEquals(closureCleanerEnabled, copy1.isClosureCleanerEnabled());
		assertEquals(forceAvroEnabled, copy1.isForceAvroEnabled());
		assertEquals(forceKryoEnabled, copy1.isForceKryoEnabled());
		assertEquals(disableGenericTypes, copy1.hasGenericTypesDisabled());
		assertEquals(objectReuseEnabled, copy1.isObjectReuseEnabled());
		assertEquals(parallelism, copy1.getParallelism());
	}

	@Test
	public void testGlobalParametersNotNull() {
		final ExecutionConfig config = new ExecutionConfig();

		assertNotNull(config.getGlobalJobParameters());
	}

	@Test
	public void testGlobalParametersHashCode() {
		ExecutionConfig config = new ExecutionConfig();
		ExecutionConfig anotherConfig = new ExecutionConfig();

		assertEquals(config.getGlobalJobParameters().hashCode(),
			anotherConfig.getGlobalJobParameters().hashCode());
	}

	@Test
	public void testReadingDefaultConfig() {
		ExecutionConfig executionConfig = new ExecutionConfig();
		Configuration configuration = new Configuration();

		// mutate config according to configuration
		executionConfig.configure(configuration, ExecutionConfigTest.class.getClassLoader());

		assertThat(executionConfig, equalTo(new ExecutionConfig()));
	}

	@Test
	public void testLoadingRegisteredKryoTypesFromConfiguration() {
		ExecutionConfig configFromSetters = new ExecutionConfig();
		configFromSetters.registerKryoType(ExecutionConfigTest.class);
		configFromSetters.registerKryoType(TestSerializer1.class);

		ExecutionConfig configFromConfiguration = new ExecutionConfig();

		Configuration configuration = new Configuration();
		configuration.setString("pipeline.registered-kryo-types", "org.apache.flink.api.common.ExecutionConfigTest;" +
					"org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1");

		// mutate config according to configuration
		configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

		assertThat(configFromConfiguration, equalTo(configFromSetters));
	}

	@Test
	public void testLoadingRegisteredPojoTypesFromConfiguration() {
		ExecutionConfig configFromSetters = new ExecutionConfig();
		configFromSetters.registerPojoType(ExecutionConfigTest.class);
		configFromSetters.registerPojoType(TestSerializer1.class);

		ExecutionConfig configFromConfiguration = new ExecutionConfig();

		Configuration configuration = new Configuration();
		configuration.setString("pipeline.registered-pojo-types", "org.apache.flink.api.common.ExecutionConfigTest;" +
			"org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1");

		// mutate config according to configuration
		configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

		assertThat(configFromConfiguration, equalTo(configFromSetters));
	}

	@Test
	public void testLoadingRestartStrategyFromConfiguration() {
		ExecutionConfig configFromSetters = new ExecutionConfig();
		configFromSetters.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			10,
			Time.minutes(2)));

		ExecutionConfig configFromConfiguration = new ExecutionConfig();

		Configuration configuration = new Configuration();
		configuration.setString("restart-strategy", "fixeddelay");
		configuration.setString("restart-strategy.fixed-delay.attempts", "10");
		configuration.setString("restart-strategy.fixed-delay.delay", "2 min");

		// mutate config according to configuration
		configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

		assertThat(configFromConfiguration, equalTo(configFromSetters));
	}

	@Test
	public void testLoadingDefaultKryoSerializersFromConfiguration() {
		ExecutionConfig configFromSetters = new ExecutionConfig();
		configFromSetters.addDefaultKryoSerializer(ExecutionConfigTest.class, TestSerializer1.class);
		configFromSetters.addDefaultKryoSerializer(TestSerializer1.class, TestSerializer2.class);

		ExecutionConfig configFromConfiguration = new ExecutionConfig();

		Configuration configuration = new Configuration();
		configuration.setString(
			"pipeline.default-kryo-serializers",
			"class:org.apache.flink.api.common.ExecutionConfigTest," +
				"serializer:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1;" +
				"class:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1," +
				"serializer:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer2");


		// mutate config according to configuration
		configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

		assertThat(configFromConfiguration, equalTo(configFromSetters));
	}

	@Test
	public void testNotOverridingRegisteredKryoTypesWithDefaultsFromConfiguration() {
		ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(ExecutionConfigTest.class);
		config.registerKryoType(TestSerializer1.class);

		Configuration configuration = new Configuration();

		// mutate config according to configuration
		config.configure(configuration, Thread.currentThread().getContextClassLoader());

		LinkedHashSet<Object> set = new LinkedHashSet<>();
		set.add(ExecutionConfigTest.class);
		set.add(TestSerializer1.class);
		assertThat(config.getRegisteredKryoTypes(), equalTo(set));
	}

	@Test
	public void testNotOverridingRegisteredPojoTypesWithDefaultsFromConfiguration() {
		ExecutionConfig config = new ExecutionConfig();
		config.registerPojoType(ExecutionConfigTest.class);
		config.registerPojoType(TestSerializer1.class);

		Configuration configuration = new Configuration();

		// mutate config according to configuration
		config.configure(configuration, Thread.currentThread().getContextClassLoader());

		LinkedHashSet<Object> set = new LinkedHashSet<>();
		set.add(ExecutionConfigTest.class);
		set.add(TestSerializer1.class);
		assertThat(config.getRegisteredPojoTypes(), equalTo(set));
	}

	@Test
	public void testNotOverridingRestartStrategiesWithDefaultsFromConfiguration() {
		ExecutionConfig config = new ExecutionConfig();
		RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
			RestartStrategies.fixedDelayRestart(
				10,
				Time.minutes(2));
		config.setRestartStrategy(restartStrategyConfiguration);

		// mutate config according to configuration
		config.configure(new Configuration(), Thread.currentThread().getContextClassLoader());

		assertThat(config.getRestartStrategy(), equalTo(restartStrategyConfiguration));
	}

	@Test
	public void testNotOverridingDefaultKryoSerializersFromConfiguration() {
		ExecutionConfig config = new ExecutionConfig();
		config.addDefaultKryoSerializer(ExecutionConfigTest.class, TestSerializer1.class);
		config.addDefaultKryoSerializer(TestSerializer1.class, TestSerializer2.class);

		Configuration configuration = new Configuration();

		// mutate config according to configuration
		config.configure(configuration, Thread.currentThread().getContextClassLoader());

		LinkedHashMap<Class<?>, Class<? extends Serializer>> serialiers = new LinkedHashMap<>();
		serialiers.put(ExecutionConfigTest.class, TestSerializer1.class);
		serialiers.put(TestSerializer1.class, TestSerializer2.class);
		assertThat(config.getDefaultKryoSerializerClasses(), equalTo(serialiers));
	}

	private static class TestSerializer1 extends Serializer<ExecutionConfigTest> implements Serializable {
		@Override
		public void write(Kryo kryo, Output output, ExecutionConfigTest object) {

		}

		@Override
		public ExecutionConfigTest read(
			Kryo kryo,
			Input input,
			Class<ExecutionConfigTest> type) {
			return null;
		}
	}

	private static class TestSerializer2 extends Serializer<TestSerializer1> implements Serializable {
		@Override
		public void write(Kryo kryo, Output output, TestSerializer1 object) {

		}

		@Override
		public TestSerializer1 read(
			Kryo kryo,
			Input input,
			Class<TestSerializer1> type) {
			return null;
		}
	}
}
