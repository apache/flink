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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TaskExecutorProcessUtils}.
 */
public class TaskExecutorProcessUtilsTest extends TestLogger {

	private static final MemorySize TASK_HEAP_SIZE = MemorySize.parse("100m");
	private static final MemorySize MANAGED_MEM_SIZE = MemorySize.parse("200m");
	private static final MemorySize TOTAL_FLINK_MEM_SIZE = MemorySize.parse("1280m");
	private static final MemorySize TOTAL_PROCESS_MEM_SIZE = MemorySize.parse("1536m");

	private static final TaskExecutorProcessSpec TM_RESOURCE_SPEC = new TaskExecutorProcessSpec(
		new CPUResource(1.0),
		MemorySize.parse("1m"),
		MemorySize.parse("2m"),
		MemorySize.parse("3m"),
		MemorySize.parse("4m"),
		MemorySize.parse("5m"),
		MemorySize.parse("6m"),
		MemorySize.parse("7m"),
		MemorySize.parse("8m"));

	private static final int NUMBER_OF_SLOTS = 2;

	private static final ResourceProfile DEFAULT_RESOURCE_PROFILE = ResourceProfile.newBuilder()
		.setCpuCores(new CPUResource(0.5))
		.setTaskHeapMemory(MemorySize.parse("3m").divide(NUMBER_OF_SLOTS))
		.setTaskOffHeapMemory(MemorySize.parse("2m"))
		.setNetworkMemory(MemorySize.parse("5m").divide(NUMBER_OF_SLOTS))
		.setManagedMemory(MemorySize.parse("3m"))
		.build();

	private static Map<String, String> oldEnvVariables;

	@Before
	public void setup() {
		oldEnvVariables = System.getenv();
	}

	@After
	public void teardown() {
		if (oldEnvVariables != null) {
			CommonTestUtils.setEnv(oldEnvVariables, true);
		}
	}

	@Test
	public void testGenerateDynamicConfigurations() {
		String dynamicConfigsStr = TaskExecutorProcessUtils.generateDynamicConfigsStr(TM_RESOURCE_SPEC);
		Map<String, String> configs = ConfigurationUtils.parseTmResourceDynamicConfigs(dynamicConfigsStr);

		assertThat(new CPUResource(Double.valueOf(configs.get(TaskManagerOptions.CPU_CORES.key()))), is(TM_RESOURCE_SPEC.getCpuCores()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key())), is(TM_RESOURCE_SPEC.getFrameworkHeapSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key())), is(TM_RESOURCE_SPEC.getFrameworkOffHeapMemorySize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.TASK_HEAP_MEMORY.key())), is(TM_RESOURCE_SPEC.getTaskHeapSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key())), is(TM_RESOURCE_SPEC.getTaskOffHeapSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.NETWORK_MEMORY_MAX.key())), is(TM_RESOURCE_SPEC.getNetworkMemSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.NETWORK_MEMORY_MIN.key())), is(TM_RESOURCE_SPEC.getNetworkMemSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.MANAGED_MEMORY_SIZE.key())), is(TM_RESOURCE_SPEC.getManagedMemorySize()));
	}

	@Test
	public void testGenerateJvmParameters() {
		String jvmParamsStr = TaskExecutorProcessUtils.generateJvmParametersStr(TM_RESOURCE_SPEC);
		Map<String, String> configs = ConfigurationUtils.parseTmResourceJvmParams(jvmParamsStr);

		assertThat(MemorySize.parse(configs.get("-Xmx")), is(TM_RESOURCE_SPEC.getFrameworkHeapSize().add(TM_RESOURCE_SPEC.getTaskHeapSize())));
		assertThat(MemorySize.parse(configs.get("-Xms")), is(TM_RESOURCE_SPEC.getFrameworkHeapSize().add(TM_RESOURCE_SPEC.getTaskHeapSize())));
		assertThat(MemorySize.parse(configs.get("-XX:MaxDirectMemorySize=")), is(TM_RESOURCE_SPEC.getFrameworkOffHeapMemorySize().add(TM_RESOURCE_SPEC.getTaskOffHeapSize()).add(TM_RESOURCE_SPEC.getNetworkMemSize())));
		assertThat(MemorySize.parse(configs.get("-XX:MaxMetaspaceSize=")), is(TM_RESOURCE_SPEC.getJvmMetaspaceSize()));
	}

	@Test
	public void testConfigCpuCores() {
		final double cpuCores = 1.0;

		Configuration conf = new Configuration();
		conf.setDouble(TaskManagerOptions.CPU_CORES, cpuCores);

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getCpuCores(), is(new CPUResource(cpuCores))));
	}

	@Test
	public void testConfigNoCpuCores() {
		Configuration conf = new Configuration();
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
		validateInAllConfigurations(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getCpuCores(), is(new CPUResource(3.0))));
	}

	@Test
	public void testConfigNegativeCpuCores() {
		Configuration conf = new Configuration();
		conf.setDouble(TaskManagerOptions.CPU_CORES, -0.1f);
		validateFailInAllConfigurations(conf);
	}

	@Test
	public void testConfigFrameworkHeapMemory() {
		final MemorySize frameworkHeapSize = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, frameworkHeapSize);

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getFrameworkHeapSize(), is(frameworkHeapSize)));
	}

	@Test
	public void testConfigFrameworkOffHeapMemory() {
		final MemorySize frameworkOffHeapSize = MemorySize.parse("10m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, frameworkOffHeapSize);

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getFrameworkOffHeapMemorySize(), is(frameworkOffHeapSize)));
	}

	@Test
	public void testConfigTaskHeapMemory() {
		final MemorySize taskHeapSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, taskHeapSize);

		// validate in configurations without explicit task heap memory size,
		// to avoid checking against overwritten task heap memory size
		validateInConfigurationsWithoutExplicitTaskHeapMem(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getTaskHeapSize(), is(taskHeapSize)));
	}

	@Test
	public void testConfigTaskOffheapMemory() {
		final MemorySize taskOffHeapSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, taskOffHeapSize);

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getTaskOffHeapSize(), is(taskOffHeapSize)));
	}

	@Test
	public void testConfigNetworkMemoryRange() {
		final MemorySize networkMin = MemorySize.parse("200m");
		final MemorySize networkMax = MemorySize.parse("500m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkMax);
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkMin);

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> {
			assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes(), greaterThanOrEqualTo(networkMin.getBytes()));
			assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes(), lessThanOrEqualTo(networkMax.getBytes()));
		});
	}

	@Test
	public void testConsistencyCheckOfDerivedNetworkMemoryWithinMinMaxRangeNotMatchingFractionPasses() {
		final Configuration configuration = setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(400);
		// set fraction to be extremely low to not match the derived network memory
		configuration.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.001f);
		// internal validation should pass
		TaskExecutorProcessUtils.processSpecFromConfig(configuration);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testConsistencyCheckOfDerivedNetworkMemoryLessThanMinFails() {
		final Configuration configuration = setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(500);
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("900m"));
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("1000m"));
		// internal validation should fail
		TaskExecutorProcessUtils.processSpecFromConfig(configuration);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testConsistencyCheckOfDerivedNetworkMemoryGreaterThanMaxFails() {
		final Configuration configuration = setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(500);
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("100m"));
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("150m"));
		// internal validation should fail
		TaskExecutorProcessUtils.processSpecFromConfig(configuration);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testConsistencyCheckOfDerivedNetworkMemoryDoesNotMatchLegacyConfigFails() {
		final int numberOfNetworkBuffers = 10;
		final int pageSizeMb = 16;
		// derive network memory which is bigger than the number of legacy network buffers
		final int networkMemorySizeMbToDerive = pageSizeMb * (numberOfNetworkBuffers + 1);
		final Configuration configuration = setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(networkMemorySizeMbToDerive);
		configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.ofMebiBytes(pageSizeMb));
		configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, numberOfNetworkBuffers);
		// internal validation should fail
		TaskExecutorProcessUtils.processSpecFromConfig(configuration);
	}

	private static Configuration setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(
			final int networkMemorySizeToDeriveMb) {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
		conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP_SIZE);
		conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED_MEM_SIZE);

		final TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(conf);
		final int derivedNetworkMemorySizeMb = taskExecutorProcessSpec.getNetworkMemSize().getMebiBytes();
		if (derivedNetworkMemorySizeMb < networkMemorySizeToDeriveMb) {
			// adjust total Flink memory size to accommodate for more network memory
			final int adjustedTotalFlinkMemoryMb = taskExecutorProcessSpec.getTotalFlinkMemorySize().getMebiBytes() -
				derivedNetworkMemorySizeMb + networkMemorySizeToDeriveMb;
			conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.ofMebiBytes(adjustedTotalFlinkMemoryMb));
		} else if (derivedNetworkMemorySizeMb > networkMemorySizeToDeriveMb) {
			// reduce derived network memory by increasing task heap size
			final int adjustedTaskHeapMemoryMb = taskExecutorProcessSpec.getTaskHeapSize().getMebiBytes() +
				derivedNetworkMemorySizeMb - networkMemorySizeToDeriveMb;
			conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(adjustedTaskHeapMemoryMb));
		}

		final TaskExecutorProcessSpec adjusteedTaskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(conf);
		assertThat(adjusteedTaskExecutorProcessSpec.getNetworkMemSize().getMebiBytes(), is(networkMemorySizeToDeriveMb));

		return conf;
	}

	@Test
	public void testConfigNetworkMemoryRangeFailure() {
		final MemorySize networkMin = MemorySize.parse("200m");
		final MemorySize networkMax = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkMax);
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkMin);

		validateFailInAllConfigurations(conf);
	}

	@Test
	public void testConfigNetworkMemoryFraction() {
		final MemorySize networkMin = MemorySize.ZERO;
		final MemorySize networkMax = MemorySize.parse("1t");
		final float fraction = 0.2f;

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkMax);
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkMin);
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, fraction);

		// validate in configurations without explicit total flink/process memory, otherwise explicit configured
		// network memory fraction might conflict with total flink/process memory minus other memory sizes
		validateInConfigWithExplicitTaskHeapAndManagedMem(conf, taskExecutorProcessSpec ->
			assertThat(taskExecutorProcessSpec.getNetworkMemSize(), is(taskExecutorProcessSpec.getTotalFlinkMemorySize().multiply(fraction))));
	}

	@Test
	public void testConfigNetworkMemoryFractionFailure() {
		Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, -0.1f);
		validateFailInAllConfigurations(conf);

		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 1.0f);
		validateFailInAllConfigurations(conf);
	}

	@Test
	public void testConfigNetworkMemoryLegacyRangeFraction() {
		final MemorySize networkMin = MemorySize.parse("200m");
		final MemorySize networkMax = MemorySize.parse("500m");

		final float fraction = 0.2f;

		@SuppressWarnings("deprecation")
		final ConfigOption<String> legacyOptionMin = NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN;
		@SuppressWarnings("deprecation")
		final ConfigOption<String> legacyOptionMax = NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX;
		@SuppressWarnings("deprecation")
		final ConfigOption<Float> legacyOptionFraction = NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION;

		Configuration conf = new Configuration();
		conf.setString(legacyOptionMin, networkMin.getMebiBytes() + "m");
		conf.setString(legacyOptionMax, networkMax.getMebiBytes() + "m");

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> {
			assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes(), greaterThanOrEqualTo(networkMin.getBytes()));
			assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes(), lessThanOrEqualTo(networkMax.getBytes()));
		});

		conf.setString(legacyOptionMin, "0m");
		conf.setString(legacyOptionMax, "1t");
		conf.setFloat(legacyOptionFraction, fraction);

		validateInConfigWithExplicitTaskHeapAndManagedMem(conf, taskExecutorProcessSpec ->
			assertThat(taskExecutorProcessSpec.getNetworkMemSize(), is(taskExecutorProcessSpec.getTotalFlinkMemorySize().multiply(fraction))));
	}

	@Test
	public void testConfigNetworkMemoryLegacyNumOfBuffers() {
		final MemorySize pageSize = MemorySize.parse("32k");
		final int numOfBuffers = 1024;
		final MemorySize networkSize = pageSize.multiply(numOfBuffers);

		@SuppressWarnings("deprecation")
		final ConfigOption<Integer> legacyOption = NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS;

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, pageSize);
		conf.setInteger(legacyOption, numOfBuffers);

		// validate in configurations without explicit total flink/process memory, otherwise explicit configured
		// network memory size might conflict with total flink/process memory minus other memory sizes
		validateInConfigWithExplicitTaskHeapAndManagedMem(conf, taskExecutorProcessSpec ->
			assertThat(taskExecutorProcessSpec.getNetworkMemSize(), is(networkSize)));
		validateInConfigurationsWithoutExplicitTaskHeapMem(conf, taskExecutorProcessSpec ->
			assertThat(taskExecutorProcessSpec.getNetworkMemSize(), is(networkSize)));
	}

	@Test
	public void testConfigManagedMemorySize() {
		final MemorySize managedMemSize = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, managedMemSize);

		// validate in configurations without explicit managed memory size,
		// to avoid checking against overwritten managed memory size
		validateInConfigurationsWithoutExplicitManagedMem(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getManagedMemorySize(), is(managedMemSize)));
	}

	@Test
	public void testConfigManagedMemoryLegacySize() {
		final MemorySize managedMemSize = MemorySize.parse("100m");

		@SuppressWarnings("deprecation")
		final ConfigOption<MemorySize> legacyOption = TaskManagerOptions.MANAGED_MEMORY_SIZE;

		Configuration conf = new Configuration();
		conf.set(legacyOption, managedMemSize);

		// validate in configurations without explicit managed memory size,
		// to avoid checking against overwritten managed memory size
		validateInConfigurationsWithoutExplicitManagedMem(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getManagedMemorySize(), is(managedMemSize)));
	}

	@Test
	public void testConfigManagedMemoryFraction() {
		final float fraction = 0.5f;

		Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, fraction);

		// managed memory fraction is only used when managed memory size is not explicitly configured
		validateInConfigurationsWithoutExplicitManagedMem(conf, taskExecutorProcessSpec ->
			assertThat(taskExecutorProcessSpec.getManagedMemorySize(), is(taskExecutorProcessSpec.getTotalFlinkMemorySize().multiply(fraction))));
	}

	@Test
	public void testConfigManagedMemoryFractionFailure() {
		final Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, -0.1f);
		validateFailInConfigurationsWithoutExplicitManagedMem(conf);

		conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 1.0f);
		validateFailInConfigurationsWithoutExplicitManagedMem(conf);
	}

	@Test
	public void testConfigJvmMetaspaceSize() {
		final MemorySize jvmMetaspaceSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.JVM_METASPACE, jvmMetaspaceSize);

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> assertThat(taskExecutorProcessSpec.getJvmMetaspaceSize(), is(jvmMetaspaceSize)));
	}

	@Test
	public void testConfigJvmOverheadRange() {
		final MemorySize minSize = MemorySize.parse("50m");
		final MemorySize maxSize = MemorySize.parse("200m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, maxSize);
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MIN, minSize);

		validateInAllConfigurations(conf, taskExecutorProcessSpec -> {
			assertThat(taskExecutorProcessSpec.getJvmOverheadSize().getBytes(),
				greaterThanOrEqualTo(minSize.getBytes()));
			assertThat(taskExecutorProcessSpec.getJvmOverheadSize().getBytes(), lessThanOrEqualTo(maxSize.getBytes()));
		});
	}

	@Test
	public void testConfigJvmOverheadRangeFailure() {
		final MemorySize minSize = MemorySize.parse("200m");
		final MemorySize maxSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, maxSize);
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MIN, minSize);

		validateFailInAllConfigurations(conf);
	}

	@Test
	public void testConfigJvmOverheadFraction() {
		final MemorySize minSize = MemorySize.ZERO;
		final MemorySize maxSize = MemorySize.parse("1t");
		final float fraction = 0.2f;

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, maxSize);
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MIN, minSize);
		conf.setFloat(TaskManagerOptions.JVM_OVERHEAD_FRACTION, fraction);

		validateInAllConfigurations(conf, taskExecutorProcessSpec ->
			assertThat(taskExecutorProcessSpec.getJvmOverheadSize(), is(taskExecutorProcessSpec.getTotalProcessMemorySize().multiply(fraction))));
	}

	@Test
	public void testConfigJvmOverheadFractionFailureNegative() {
		final Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.JVM_OVERHEAD_FRACTION, -0.1f);
		validateFailInConfigurationsWithoutExplicitManagedMem(conf);
	}

	@Test
	public void testConfigJvmOverheadFractionFailureNoLessThanOne() {
		final Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.JVM_OVERHEAD_FRACTION, 1.0f);
		validateFailInConfigurationsWithoutExplicitManagedMem(conf);
	}

	@Test
	public void testConfigJvmOverheadDeriveFromProcessAndFlinkMemorySize() {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1000m"));
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("800m"));
		conf.set(TaskManagerOptions.JVM_METASPACE, MemorySize.parse("100m"));
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.parse("50m"));
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.parse("200m"));
		conf.set(TaskManagerOptions.JVM_OVERHEAD_FRACTION, 0.5f);

		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(conf);
		assertThat(taskExecutorProcessSpec.getJvmOverheadSize(), is(MemorySize.parse("100m")));
	}

	@Test
	public void testConfigJvmOverheadDeriveFromProcessAndFlinkMemorySizeFailure() {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1000m"));
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("800m"));
		conf.set(TaskManagerOptions.JVM_METASPACE, MemorySize.parse("100m"));
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.parse("150m"));
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.parse("200m"));
		conf.set(TaskManagerOptions.JVM_OVERHEAD_FRACTION, 0.5f);

		validateFail(conf);
	}

	@Test
	public void testConfigTotalFlinkMemory() {
		final MemorySize totalFlinkMemorySize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemorySize);

		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(conf);
		assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize(), is(totalFlinkMemorySize));
	}

	@Test
	public void testFlinkInternalMemorySizeAddUpFailure() {
		final MemorySize totalFlinkMemory = MemorySize.parse("499m");
		final MemorySize frameworkHeap = MemorySize.parse("100m");
		final MemorySize taskHeap = MemorySize.parse("100m");
		final MemorySize taskOffHeap = MemorySize.parse("100m");
		final MemorySize network = MemorySize.parse("100m");
		final MemorySize managed = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
		conf.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, frameworkHeap);
		conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, taskHeap);
		conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, taskOffHeap);
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, network);
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, network);
		conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, managed);

		validateFail(conf);
	}

	@Test
	public void testConfigTotalProcessMemorySize() {
		final MemorySize totalProcessMemorySize = MemorySize.parse("2g");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalProcessMemorySize);

		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(conf);
		assertThat(taskExecutorProcessSpec.getTotalProcessMemorySize(), is(totalProcessMemorySize));
	}

	@Test
	public void testConfigLegacyTaskManagerHeapSize() {
		final MemorySize taskManagerHeapSize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, taskManagerHeapSize);

		testConfigLegacyTaskManagerHeapMemory(conf, taskManagerHeapSize);
	}

	@Test
	public void testConfigLegacyTaskManagerHeapMB() {
		final MemorySize taskManagerHeapSize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB, taskManagerHeapSize.getMebiBytes());

		testConfigLegacyTaskManagerHeapMemory(conf, taskManagerHeapSize);
	}

	@Test
	public void testConfigLegacyTaskManagerHeapEnv() {
		final MemorySize taskManagerHeapSize = MemorySize.parse("1g");

		final Map<String, String> env = new HashMap<>();
		env.put("FLINK_TM_HEAP", "1g");
		CommonTestUtils.setEnv(env);

		testConfigLegacyTaskManagerHeapMemory(new Configuration(), taskManagerHeapSize);
	}

	@Test
	public void testConfigBothTotalFlinkSizeAndLegacyTaskManagerHeapSize() {
		final MemorySize totalFlinkSize = MemorySize.parse("1g");
		final MemorySize taskManagerHeapSize = MemorySize.parse("2g");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkSize);
		conf.set(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, taskManagerHeapSize);

		testConfigLegacyTaskManagerHeapMemoryStandalone(conf, totalFlinkSize);
	}

	@Test
	public void testConfigBothTotalProcessSizeAndLegacyTaskManagerHeapSize() {
		final MemorySize totalProcess = MemorySize.parse("1g");
		final MemorySize taskManagerHeapSize = MemorySize.parse("2g");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalProcess);
		conf.set(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, taskManagerHeapSize);

		testConfigLegacyTaskManagerHeapMemoryContainerized(conf, totalProcess);
	}

	private void testConfigLegacyTaskManagerHeapMemory(final Configuration configuration, final MemorySize expected) {
		testConfigLegacyTaskManagerHeapMemoryStandalone(configuration, expected);
		testConfigLegacyTaskManagerHeapMemoryContainerized(configuration, expected);
	}

	private void testConfigLegacyTaskManagerHeapMemoryStandalone(final Configuration configuration, final MemorySize expected) {
		final Configuration adjustedConfig = TaskExecutorProcessUtils
			.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(configuration, TaskManagerOptions.TOTAL_FLINK_MEMORY);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(adjustedConfig);
		assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize(), is(expected));
	}

	private void testConfigLegacyTaskManagerHeapMemoryContainerized(final Configuration configuration, final MemorySize expected) {
		final Configuration adjustedConfig = TaskExecutorProcessUtils
			.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(configuration, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(adjustedConfig);
		assertThat(taskExecutorProcessSpec.getTotalProcessMemorySize(), is(expected));
	}

	@Test
	public void testFlinkInternalMemoryFractionAddUpFailure() {
		final float networkFraction = 0.6f;
		final float managedFraction = 0.6f;

		Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, networkFraction);
		conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, managedFraction);

		// if managed memory size is explicitly configured, then managed memory fraction will be ignored
		validateFailInConfigWithExplicitTotalFlinkMem(conf);
		validateFailInConfigWithExplicitTotalProcessMem(conf);
	}

	@Test
	public void testConfigTotalProcessMemoryLegacySize() {
		final MemorySize totalProcessMemorySize = MemorySize.parse("2g");

		@SuppressWarnings("deprecation")
		final ConfigOption<MemorySize> legacyOption = TaskManagerOptions.TOTAL_PROCESS_MEMORY;

		Configuration conf = new Configuration();
		conf.set(legacyOption, totalProcessMemorySize);

		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(conf);
		assertThat(taskExecutorProcessSpec.getTotalProcessMemorySize(), is(totalProcessMemorySize));
	}

	@Test
	public void testConfigTotalProcessMemoryAddUpFailure() {
		final MemorySize totalProcessMemory = MemorySize.parse("699m");
		final MemorySize totalFlinkMemory = MemorySize.parse("500m");
		final MemorySize jvmMetaspace = MemorySize.parse("100m");
		final MemorySize jvmOverhead = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalProcessMemory);
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
		conf.set(TaskManagerOptions.JVM_METASPACE, jvmMetaspace);
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MIN, jvmOverhead);
		conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, jvmOverhead);

		validateFail(conf);
	}

	@Test
	public void testCreateDefaultWorkerSlotProfiles() {
		assertThat(
			TaskExecutorProcessUtils.createDefaultWorkerSlotProfiles(TM_RESOURCE_SPEC, NUMBER_OF_SLOTS),
			is(Arrays.asList(DEFAULT_RESOURCE_PROFILE, DEFAULT_RESOURCE_PROFILE)));
	}

	@Test
	public void testGenerateDefaultSlotProfile() {
		assertThat(
			TaskExecutorProcessUtils.generateDefaultSlotResourceProfile(TM_RESOURCE_SPEC, NUMBER_OF_SLOTS),
			is(DEFAULT_RESOURCE_PROFILE));
	}

	@Test
	public void testExceptionShouldContainRequiredConfigOptions() {
		try {
			TaskExecutorProcessUtils.processSpecFromConfig(new Configuration());
		} catch (final IllegalConfigurationException e) {
			assertThat(e.getMessage(), containsString(TaskManagerOptions.TASK_HEAP_MEMORY.key()));
			assertThat(e.getMessage(), containsString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key()));
			assertThat(e.getMessage(), containsString(TaskManagerOptions.TOTAL_FLINK_MEMORY.key()));
			assertThat(e.getMessage(), containsString(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key()));
		}
	}

	private void validateInAllConfigurations(final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		validateInConfigWithExplicitTaskHeapAndManagedMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkAndManagedMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
	}

	private void validateFailInAllConfigurations(final Configuration customConfig) {
		validateFailInConfigWithExplicitTaskHeapAndManagedMem(customConfig);
		validateFailInConfigWithExplicitTotalFlinkMem(customConfig);
		validateFailInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig);
		validateFailInConfigWithExplicitTotalFlinkAndManagedMem(customConfig);
		validateFailInConfigWithExplicitTotalProcessMem(customConfig);
	}

	private void validateInConfigurationsWithoutExplicitTaskHeapMem(final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkAndManagedMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
	}

	private void validateInConfigurationsWithoutExplicitManagedMem(final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
	}

	private void validateFailInConfigurationsWithoutExplicitManagedMem(final Configuration customConfig) {
		validateFailInConfigWithExplicitTotalFlinkMem(customConfig);
		validateFailInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig);
		validateFailInConfigWithExplicitTotalProcessMem(customConfig);
	}

	private void validateInConfigWithExplicitTaskHeapAndManagedMem(
		final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit task heap and managed memory size.");
		final Configuration config = configWithExplicitTaskHeapAndManageMem();
		config.addAll(customConfig);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(config);
		assertThat(taskExecutorProcessSpec.getTaskHeapSize(), is(TASK_HEAP_SIZE));
		assertThat(taskExecutorProcessSpec.getManagedMemorySize(), is(MANAGED_MEM_SIZE));
		validateFunc.accept(taskExecutorProcessSpec);
	}

	private void validateFailInConfigWithExplicitTaskHeapAndManagedMem(final Configuration customConfig) {
		log.info("Validating failing in configuration with explicit task heap and managed memory size.");
		final Configuration config = configWithExplicitTaskHeapAndManageMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateInConfigWithExplicitTotalFlinkMem(
		final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit total flink memory size.");
		final Configuration config = configWithExplicitTotalFlinkMem();
		config.addAll(customConfig);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(config);
		assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize(), is(TOTAL_FLINK_MEM_SIZE));
		validateFunc.accept(taskExecutorProcessSpec);
	}

	private void validateFailInConfigWithExplicitTotalFlinkMem(final Configuration customConfig) {
		log.info("Validating failing in configuration with explicit total flink memory size.");
		final Configuration config = configWithExplicitTotalFlinkMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateInConfigWithExplicitTotalFlinkAndTaskHeapMem(
		final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit total flink and task heap memory size.");
		final Configuration config = configWithExplicitTotalFlinkAndTaskHeapMem();
		config.addAll(customConfig);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(config);
		assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize(), is(TOTAL_FLINK_MEM_SIZE));
		assertThat(taskExecutorProcessSpec.getTaskHeapSize(), is(TASK_HEAP_SIZE));
		validateFunc.accept(taskExecutorProcessSpec);
	}

	private void validateFailInConfigWithExplicitTotalFlinkAndTaskHeapMem(final Configuration customConfig) {
		log.info("Validating failing in configuration with explicit total flink and task heap memory size.");
		final Configuration config = configWithExplicitTotalFlinkAndTaskHeapMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateInConfigWithExplicitTotalFlinkAndManagedMem(
		final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit total flink and managed memory size.");
		final Configuration config = configWithExplicitTotalFlinkAndManagedMem();
		config.addAll(customConfig);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(config);
		assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize(), is(TOTAL_FLINK_MEM_SIZE));
		assertThat(taskExecutorProcessSpec.getManagedMemorySize(), is(MANAGED_MEM_SIZE));
		validateFunc.accept(taskExecutorProcessSpec);
	}

	private void validateFailInConfigWithExplicitTotalFlinkAndManagedMem(final Configuration customConfig) {
		log.info("Validating failing in configuration with explicit total flink and managed memory size.");
		final Configuration config = configWithExplicitTotalFlinkAndManagedMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateInConfigWithExplicitTotalProcessMem(
		final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit total process memory size.");
		final Configuration config = configWithExplicitTotalProcessMem();
		config.addAll(customConfig);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(config);
		assertThat(taskExecutorProcessSpec.getTotalProcessMemorySize(), is(TOTAL_PROCESS_MEM_SIZE));
		validateFunc.accept(taskExecutorProcessSpec);
	}

	private void validateFailInConfigWithExplicitTotalProcessMem(final Configuration customConfig) {
		log.info("Validating failing in configuration with explicit total process memory size.");
		final Configuration config = configWithExplicitTotalProcessMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateFail(final Configuration config) {
		try {
			TaskExecutorProcessUtils.processSpecFromConfig(config);
			fail("Configuration did not fail as expected.");
		} catch (IllegalConfigurationException e) {
			// expected
		}
	}

	private static Configuration configWithExplicitTaskHeapAndManageMem() {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP_SIZE);
		conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED_MEM_SIZE);
		return conf;
	}

	private static Configuration configWithExplicitTotalFlinkMem() {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
		return conf;
	}

	private static Configuration configWithExplicitTotalFlinkAndTaskHeapMem() {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
		conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP_SIZE);
		return conf;
	}

	private static Configuration configWithExplicitTotalFlinkAndManagedMem() {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
		conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED_MEM_SIZE);
		return conf;
	}

	private static Configuration configWithExplicitTotalProcessMem() {
		final Configuration conf = new Configuration();
		conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_MEM_SIZE);
		return conf;
	}
}
