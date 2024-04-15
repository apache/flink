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
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.JM_LEGACY_HEAP_OPTIONS;
import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.JM_PROCESS_MEMORY_OPTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for {@link JobManagerProcessUtils}. */
class JobManagerProcessUtilsTest extends ProcessMemoryUtilsTestBase<JobManagerProcessSpec> {

    private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcessUtilsTest.class);

    private static final MemorySize JVM_HEAP_SIZE = MemorySize.parse("1152m");
    private static final MemorySize TOTAL_FLINK_MEM_SIZE = MemorySize.parse("1280m");
    private static final MemorySize TOTAL_PROCESS_MEM_SIZE = MemorySize.parse("1536m");

    @RegisterExtension
    private final LoggerAuditingExtension testLoggerResource =
            new LoggerAuditingExtension(JobManagerFlinkMemoryUtils.class, Level.INFO);

    public JobManagerProcessUtilsTest() {
        super(
                JM_PROCESS_MEMORY_OPTIONS,
                JM_LEGACY_HEAP_OPTIONS,
                JobManagerOptions.TOTAL_PROCESS_MEMORY);
    }

    @Test
    void testGenerateDynamicConfigurations() {
        Configuration config = new Configuration();
        config.set(JobManagerOptions.JVM_HEAP_MEMORY, MemorySize.parse("1m"));
        config.set(JobManagerOptions.OFF_HEAP_MEMORY, MemorySize.parse("2m"));
        config.set(JobManagerOptions.JVM_METASPACE, MemorySize.parse("3m"));
        config.set(JobManagerOptions.JVM_OVERHEAD_MIN, MemorySize.parse("4m"));
        config.set(JobManagerOptions.JVM_OVERHEAD_MAX, MemorySize.parse("5m"));
        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(config);

        String dynamicConfigsStr =
                JobManagerProcessUtils.generateDynamicConfigsStr(jobManagerProcessSpec);
        Map<String, String> configs =
                parseAndAssertJobManagerResourceDynamicConfig(dynamicConfigsStr);

        assertThat(MemorySize.parse(configs.get(JobManagerOptions.JVM_HEAP_MEMORY.key())))
                .isEqualTo(jobManagerProcessSpec.getJvmHeapMemorySize());
        assertThat(MemorySize.parse(configs.get(JobManagerOptions.OFF_HEAP_MEMORY.key())))
                .isEqualTo(jobManagerProcessSpec.getJvmDirectMemorySize());
        assertThat(MemorySize.parse(configs.get(JobManagerOptions.JVM_METASPACE.key())))
                .isEqualTo(jobManagerProcessSpec.getJvmMetaspaceSize());
        assertThat(MemorySize.parse(configs.get(JobManagerOptions.JVM_OVERHEAD_MIN.key())))
                .isEqualTo(jobManagerProcessSpec.getJvmOverheadSize());
        assertThat(MemorySize.parse(configs.get(JobManagerOptions.JVM_OVERHEAD_MAX.key())))
                .isEqualTo(jobManagerProcessSpec.getJvmOverheadSize());
    }

    private static Map<String, String> parseAndAssertJobManagerResourceDynamicConfig(
            String dynamicParameterStr) {
        Map<String, String> config = new HashMap<>();
        String[] dynamicParameterTokens = dynamicParameterStr.split(" ");

        assertThat(dynamicParameterTokens.length % 2).isZero();
        for (int i = 0; i < dynamicParameterTokens.length; ++i) {
            String configStr = dynamicParameterTokens[i];
            if (i % 2 == 0) {
                assertThat(configStr).isEqualTo("-D");
            } else {
                String[] configEntry = configStr.split("=");
                assertThat(configEntry).hasSize(2);
                config.put(configEntry[0], configEntry[1]);
            }
        }

        assertThat(config).containsKey(JobManagerOptions.JVM_HEAP_MEMORY.key());
        assertThat(config).containsKey(JobManagerOptions.OFF_HEAP_MEMORY.key());
        assertThat(config).containsKey(JobManagerOptions.JVM_METASPACE.key());
        assertThat(config).containsKey(JobManagerOptions.JVM_OVERHEAD_MIN.key());
        assertThat(config).containsKey(JobManagerOptions.JVM_OVERHEAD_MAX.key());

        return config;
    }

    @Test
    void testConfigJvmHeapMemory() {
        MemorySize jvmHeapSize = MemorySize.parse("50m");

        Configuration conf = new Configuration();
        conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeapSize);

        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(conf);
        assertThat(jobManagerProcessSpec.getJvmHeapMemorySize()).isEqualTo(jvmHeapSize);
    }

    @Test
    void testLogFailureOfJvmHeapSizeMinSizeVerification() {
        MemorySize jvmHeapMemory = MemorySize.parse("50m");

        Configuration conf = new Configuration();
        conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeapMemory);

        JobManagerProcessUtils.processSpecFromConfig(conf);

        assertThat(testLoggerResource.getMessages())
                .anyMatch(
                        str ->
                                str.contains(
                                        String.format(
                                                "The configured or derived JVM heap memory size (%s) is less than its recommended minimum value (%s)",
                                                jvmHeapMemory.toHumanReadableString(),
                                                JobManagerOptions.MIN_JVM_HEAP_SIZE
                                                        .toHumanReadableString())));
    }

    @Test
    void testConfigOffHeapMemory() {
        MemorySize offHeapMemory = MemorySize.parse("100m");

        Configuration conf = new Configuration();
        conf.set(JobManagerOptions.OFF_HEAP_MEMORY, offHeapMemory);

        validateInAllConfigurationsWithoutExplicitTotalFlinkAndJvmHeapMem(
                conf,
                jobManagerProcessSpec ->
                        assertThat(jobManagerProcessSpec.getJvmDirectMemorySize())
                                .isEqualTo(offHeapMemory));
    }

    @Test
    void testFlinkInternalMemorySizeAddUpFailure() {
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
    void testJvmHeapExceedsTotalFlinkMemoryFailure() {
        MemorySize totalFlinkMemory = MemorySize.ofMebiBytes(100);
        MemorySize jvmHeap = MemorySize.ofMebiBytes(150);

        Configuration conf = new Configuration();
        conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
        conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeap);

        validateFail(conf);
    }

    @Test
    void testOffHeapMemoryDerivedFromJvmHeapAndTotalFlinkMemory() {
        MemorySize jvmHeap = MemorySize.ofMebiBytes(150);
        MemorySize defaultOffHeap = JobManagerOptions.OFF_HEAP_MEMORY.defaultValue();
        MemorySize expectedOffHeap = MemorySize.ofMebiBytes(100).add(defaultOffHeap);
        MemorySize totalFlinkMemory = jvmHeap.add(expectedOffHeap);

        Configuration conf = new Configuration();
        conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
        conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeap);

        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(conf);
        assertThat(jobManagerProcessSpec.getJvmDirectMemorySize()).isEqualTo(expectedOffHeap);
        assertThat(testLoggerResource.getMessages())
                .anyMatch(
                        str ->
                                str.contains(
                                        String.format(
                                                "The Off-Heap Memory size (%s) is derived the configured Total Flink Memory size (%s) minus "
                                                        + "the configured JVM Heap Memory size (%s). The default Off-Heap Memory size (%s) is ignored.",
                                                expectedOffHeap.toHumanReadableString(),
                                                totalFlinkMemory.toHumanReadableString(),
                                                jvmHeap.toHumanReadableString(),
                                                defaultOffHeap.toHumanReadableString())));
    }

    @Test
    void testDeriveFromRequiredFineGrainedOptions() {
        MemorySize jvmHeap = MemorySize.ofMebiBytes(150);
        MemorySize offHeap = MemorySize.ofMebiBytes(50);
        MemorySize totalFlinkMemory = MemorySize.ofMebiBytes(200);
        MemorySize expectedOffHeap = MemorySize.ofMebiBytes(50);

        Configuration conf = new Configuration();
        conf.set(JobManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
        conf.set(JobManagerOptions.OFF_HEAP_MEMORY, offHeap);
        conf.set(JobManagerOptions.JVM_HEAP_MEMORY, jvmHeap);

        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(conf);
        assertThat(jobManagerProcessSpec.getJvmDirectMemorySize()).isEqualTo(expectedOffHeap);
    }

    @Override
    protected JobManagerProcessSpec processSpecFromConfig(Configuration config) {
        return JobManagerProcessUtils.processSpecFromConfig(config);
    }

    @Override
    protected Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
            Configuration config) {
        return JobManagerProcessUtils.getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
                config, getNewOptionForLegacyHeapOption());
    }

    @Override
    protected void validateInAllConfigurations(
            Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
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
            Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
        validateInConfigWithExplicitJvmHeap(customConfig, validateFunc);
        validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
    }

    private void validateInConfigWithExplicitJvmHeap(
            Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
        LOG.info("Validating in configuration with explicit jvm heap.");
        Configuration config = configWithExplicitJvmHeap();
        config.addAll(customConfig);
        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(config);
        assertThat(jobManagerProcessSpec.getJvmHeapMemorySize()).isEqualTo(JVM_HEAP_SIZE);
        validateFunc.accept(jobManagerProcessSpec);
    }

    private void validateFailInConfigWithExplicitJvmHeap(Configuration customConfig) {
        LOG.info("Validating failing in configuration with explicit jvm heap.");
        Configuration config = configWithExplicitJvmHeap();
        config.addAll(customConfig);
        validateFail(config);
    }

    private void validateInConfigWithExplicitTotalFlinkMem(
            Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
        LOG.info("Validating in configuration with explicit total flink memory size.");
        Configuration config = configWithExplicitTotalFlinkMem();
        config.addAll(customConfig);
        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(config);
        assertThat(jobManagerProcessSpec.getTotalFlinkMemorySize()).isEqualTo(TOTAL_FLINK_MEM_SIZE);
        validateFunc.accept(jobManagerProcessSpec);
    }

    private void validateFailInConfigWithExplicitTotalFlinkMem(Configuration customConfig) {
        LOG.info("Validating failing in configuration with explicit total flink memory size.");
        Configuration config = configWithExplicitTotalFlinkMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    private void validateInConfigWithExplicitTotalFlinkAndJvmHeapMem(
            Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
        LOG.info("Validating in configuration with explicit total flink and jvm heap memory size.");
        Configuration config = configWithExplicitTotalFlinkAndJvmHeapMem();
        config.addAll(customConfig);
        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(config);
        assertThat(jobManagerProcessSpec.getTotalFlinkMemorySize()).isEqualTo(TOTAL_FLINK_MEM_SIZE);
        assertThat(jobManagerProcessSpec.getJvmHeapMemorySize()).isEqualTo(JVM_HEAP_SIZE);
        validateFunc.accept(jobManagerProcessSpec);
    }

    private void validateFailInConfigWithExplicitTotalFlinkAndJvmHeapMem(
            Configuration customConfig) {
        LOG.info(
                "Validating failing in configuration with explicit total flink and jvm heap memory size.");
        Configuration config = configWithExplicitTotalFlinkAndJvmHeapMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    private void validateInConfigWithExplicitTotalProcessMem(
            Configuration customConfig, Consumer<JobManagerProcessSpec> validateFunc) {
        LOG.info("Validating in configuration with explicit total process memory size.");
        Configuration config = configWithExplicitTotalProcessMem();
        config.addAll(customConfig);
        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfig(config);
        assertThat(jobManagerProcessSpec.getTotalProcessMemorySize())
                .isEqualTo(TOTAL_PROCESS_MEM_SIZE);
        validateFunc.accept(jobManagerProcessSpec);
    }

    private void validateFailInConfigWithExplicitTotalProcessMem(Configuration customConfig) {
        LOG.info("Validating failing in configuration with explicit total process memory size.");
        Configuration config = configWithExplicitTotalProcessMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    @Override
    protected void validateFail(Configuration config) {
        assertThatExceptionOfType(IllegalConfigurationException.class)
                .as("Configuration did not fail as expected.")
                .isThrownBy(() -> JobManagerProcessUtils.processSpecFromConfig(config));
    }

    @Override
    protected void configWithFineGrainedOptions(
            Configuration configuration, MemorySize totalFlinkMemorySize) {
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
