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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtilsTestBase;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemoryUtils;
import org.apache.flink.testutils.logging.TestLoggerResource;

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.function.Consumer;

import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.JM_LEGACY_HEAP_OPTIONS;
import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.JM_PROCESS_MEMORY_OPTIONS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JobManagerProcessUtils}.
 */
public class JobManagerProcessUtilsTest extends ProcessMemoryUtilsTestBase<JobManagerProcessSpec> {
	private static final MemorySize JVM_HEAP_SIZE = MemorySize.parse("1152m");
	private static final MemorySize TOTAL_FLINK_MEM_SIZE = MemorySize.parse("1280m");
	private static final MemorySize TOTAL_PROCESS_MEM_SIZE = MemorySize.parse("1536m");

	@Rule
	public final TestLoggerResource testLoggerResource = new TestLoggerResource(JobManagerFlinkMemoryUtils.class, Level.INFO);

	public JobManagerProcessUtilsTest() {
		super(JM_PROCESS_MEMORY_OPTIONS, JM_LEGACY_HEAP_OPTIONS, JobManagerOptions.TOTAL_PROCESS_MEMORY);
	}

	@Test
	public void testConfigJvmHeapMemory() {
		MemorySize jvmHeapSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeapSize);

		JobManagerProcessSpec jobManagerProcessSpec = JobManagerProcessUtils.processSpecFromConfig(conf);
		assertThat(jobManagerProcessSpec.getJvmHeapMemorySize(), is(jvmHeapSize));
	}

	@Test
	public void testLogFailureOfJvmHeapSizeMinSizeVerification() {
		MemorySize jvmHeapMemory = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeapMemory);

		JobManagerProcessUtils.processSpecFromConfig(conf);
		MatcherAssert.assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString(String.format(
				"The configured or derived JVM heap memory size (%s) is less than its recommended minimum value (%s)",
				jvmHeapMemory.toHumanReadableString(),
				JobManagerOptions.MIN_JVM_HEAP_SIZE.toHumanReadableString()))));
	}

	@Test
	public void testConfigOffHeapMemory() {
		MemorySize offHeapMemory = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.OFF_HEAP_MEMORY, offHeapMemory);

		validateInAllConfigurationsWithoutExplicitTotalFlinkAndJvmHeapMem(
			conf,
			jobManagerProcessSpec -> assertThat(jobManagerProcessSpec.getJvmDirectMemorySize(), is(offHeapMemory)));
	}

	@Test
	public void testFlinkInternalMemorySizeAddUpFailure() {
		MemorySize totalFlinkMemory = MemorySize.parse("199m");
		MemorySize jvmHeap = MemorySize.parse("100m");
		MemorySize offHeapMemory = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
		conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeap);
		conf.set(JobManagerOptions.OFF_HEAP_MEMORY, offHeapMemory);

		validateFail(conf);
	}

	@Test
	public void testJvmHeapExceedsTotalFlinkMemoryFailure() {
		MemorySize totalFlinkMemory = MemorySize.ofMebiBytes(100);
		MemorySize jvmHeap = MemorySize.ofMebiBytes(150);

		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
		conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeap);

		validateFail(conf);
	}

	@Test
	public void testOffHeapMemoryDerivedFromJvmHeapAndTotalFlinkMemory() {
		MemorySize jvmHeap = MemorySize.ofMebiBytes(150);
		MemorySize defaultOffHeap = JobManagerOptions.OFF_HEAP_MEMORY.defaultValue();
		MemorySize expectedOffHeap = MemorySize.ofMebiBytes(100).add(defaultOffHeap);
		MemorySize totalFlinkMemory = jvmHeap.add(expectedOffHeap);

		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
		conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeap);

		JobManagerProcessSpec jobManagerProcessSpec = JobManagerProcessUtils.processSpecFromConfig(conf);
		assertThat(jobManagerProcessSpec.getJvmDirectMemorySize(), is(expectedOffHeap));
		MatcherAssert.assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString(String.format(
				"The Off-Heap Memory size (%s) is derived the configured Total Flink Memory size (%s) minus " +
					"the configured JVM Heap Memory size (%s). The default Off-Heap Memory size (%s) is ignored.",
				expectedOffHeap.toHumanReadableString(),
				totalFlinkMemory.toHumanReadableString(),
				jvmHeap.toHumanReadableString(),
				defaultOffHeap.toHumanReadableString()))));
	}

	@Override
	protected JobManagerProcessSpec processSpecFromConfig(Configuration config) {
		return JobManagerProcessUtils.processSpecFromConfig(config);
	}

	@Override
	protected Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(Configuration config) {
		return JobManagerProcessUtils.getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
			config,
			getNewOptionForLegacyHeapOption());
	}

	@Override
	protected void validateInAllConfigurations(Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
		validateInConfigWithExplicitJvmHeap(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkAndJvmHeapMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
	}

	@Override
	protected void validateFailInAllConfigurations(Configuration customConfig) {
		validateFailInConfigWithExplicitJvmHeap(customConfig);
		validateFailInConfigWithExplicitTotalFlinkMem(customConfig);
		validateFailInConfigWithExplicitTotalFlinkAndJvmHeapMem(customConfig);
		validateFailInConfigWithExplicitTotalProcessMem(customConfig);
	}

	private void validateInAllConfigurationsWithoutExplicitTotalFlinkAndJvmHeapMem(
			Configuration customConfig,
			Consumer<JobManagerProcessSpec> validateFunc) {
		validateInConfigWithExplicitJvmHeap(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
	}

	private void validateInConfigWithExplicitJvmHeap(
		Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit jvm heap.");
		Configuration config = configWithExplicitJvmHeap();
		config.addAll(customConfig);
		JobManagerProcessSpec jobManagerProcessSpec = JobManagerProcessUtils.processSpecFromConfig(config);
		assertThat(jobManagerProcessSpec.getJvmHeapMemorySize(), is(JVM_HEAP_SIZE));
		validateFunc.accept(jobManagerProcessSpec);
	}

	private void validateFailInConfigWithExplicitJvmHeap(Configuration customConfig) {
		log.info("Validating failing in configuration with explicit jvm heap.");
		Configuration config = configWithExplicitJvmHeap();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateInConfigWithExplicitTotalFlinkMem(
		Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit total flink memory size.");
		Configuration config = configWithExplicitTotalFlinkMem();
		config.addAll(customConfig);
		JobManagerProcessSpec jobManagerProcessSpec = JobManagerProcessUtils.processSpecFromConfig(config);
		assertThat(jobManagerProcessSpec.getTotalFlinkMemorySize(), is(TOTAL_FLINK_MEM_SIZE));
		validateFunc.accept(jobManagerProcessSpec);
	}

	private void validateFailInConfigWithExplicitTotalFlinkMem(Configuration customConfig) {
		log.info("Validating failing in configuration with explicit total flink memory size.");
		Configuration config = configWithExplicitTotalFlinkMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateInConfigWithExplicitTotalFlinkAndJvmHeapMem(
			Configuration customConfig,
			Consumer<JobManagerProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit total flink and jvm heap memory size.");
		Configuration config = configWithExplicitTotalFlinkAndJvmHeapMem();
		config.addAll(customConfig);
		JobManagerProcessSpec jobManagerProcessSpec = JobManagerProcessUtils.processSpecFromConfig(config);
		assertThat(jobManagerProcessSpec.getTotalFlinkMemorySize(), is(TOTAL_FLINK_MEM_SIZE));
		assertThat(jobManagerProcessSpec.getJvmHeapMemorySize(), is(JVM_HEAP_SIZE));
		validateFunc.accept(jobManagerProcessSpec);
	}

	private void validateFailInConfigWithExplicitTotalFlinkAndJvmHeapMem(Configuration customConfig) {
		log.info("Validating failing in configuration with explicit total flink and jvm heap memory size.");
		Configuration config = configWithExplicitTotalFlinkAndJvmHeapMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	private void validateInConfigWithExplicitTotalProcessMem(
		Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
		log.info("Validating in configuration with explicit total process memory size.");
		Configuration config = configWithExplicitTotalProcessMem();
		config.addAll(customConfig);
		JobManagerProcessSpec jobManagerProcessSpec = JobManagerProcessUtils.processSpecFromConfig(config);
		assertThat(jobManagerProcessSpec.getTotalProcessMemorySize(), is(TOTAL_PROCESS_MEM_SIZE));
		validateFunc.accept(jobManagerProcessSpec);
	}

	private void validateFailInConfigWithExplicitTotalProcessMem(Configuration customConfig) {
		log.info("Validating failing in configuration with explicit total process memory size.");
		Configuration config = configWithExplicitTotalProcessMem();
		config.addAll(customConfig);
		validateFail(config);
	}

	@Override
	protected void validateFail(Configuration config) {
		try {
			JobManagerProcessUtils.processSpecFromConfig(config);
			fail("Configuration did not fail as expected.");
		} catch (IllegalConfigurationException e) {
			// expected
		}
	}

	@Override
	protected void configWithFineGrainedOptions(Configuration configuration, MemorySize totalFlinkMemorySize) {
		MemorySize heapSize = new MemorySize(totalFlinkMemorySize.getBytes() / 2);
		MemorySize offHeapSize = totalFlinkMemorySize.subtract(heapSize);
		configuration.set(JobManagerOptions.JVM_HEAP_MEMORY, heapSize);
		configuration.set(JobManagerOptions.OFF_HEAP_MEMORY, offHeapSize);
	}

	private static Configuration configWithExplicitJvmHeap() {
		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.JVM_HEAP_MEMORY, JVM_HEAP_SIZE);
		return conf;
	}

	private static Configuration configWithExplicitTotalFlinkMem() {
		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
		return conf;
	}

	private static Configuration configWithExplicitTotalFlinkAndJvmHeapMem() {
		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
		conf.set(JobManagerOptions.JVM_HEAP_MEMORY, JVM_HEAP_SIZE);
		return conf;
	}

	private static Configuration configWithExplicitTotalProcessMem() {
		Configuration conf = new Configuration();
		conf.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_MEM_SIZE);
		return conf;
	}
}
