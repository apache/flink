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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base test suite for JM/TM memory calculations to test common methods in {@link
 * ProcessMemoryUtils}.
 */
public abstract class ProcessMemoryUtilsTestBase<T extends ProcessMemorySpec> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private static Map<String, String> oldEnvVariables;

    private final ProcessMemoryOptions options;
    private final LegacyMemoryOptions legacyMemoryOptions;
    private final ConfigOption<MemorySize> newOptionForLegacyHeapOption;

    @SuppressWarnings("JUnitTestCaseWithNonTrivialConstructors")
    protected ProcessMemoryUtilsTestBase(
            ProcessMemoryOptions options,
            LegacyMemoryOptions legacyMemoryOptions,
            ConfigOption<MemorySize> newOptionForLegacyHeapOption) {
        this.options = checkNotNull(options);
        this.legacyMemoryOptions = checkNotNull(legacyMemoryOptions);
        this.newOptionForLegacyHeapOption = checkNotNull(newOptionForLegacyHeapOption);
    }

    @BeforeEach
    void setup() {
        oldEnvVariables = System.getenv();
    }

    @AfterEach
    void teardown() {
        if (oldEnvVariables != null) {
            CommonTestUtils.setEnv(oldEnvVariables, true);
        }
    }

    @Test
    void testGenerateJvmParameters() {
        ProcessMemorySpec spec = JvmArgTestingProcessMemorySpec.generate();
        String jvmParamsStr = ProcessMemoryUtils.generateJvmParametersStr(spec, true);
        Map<String, String> configs = ConfigurationUtils.parseJvmArgString(jvmParamsStr);

        assertThat(configs).hasSize(4);
        assertThat(MemorySize.parse(configs.get("-Xmx"))).isEqualTo(spec.getJvmHeapMemorySize());
        assertThat(MemorySize.parse(configs.get("-Xms"))).isEqualTo(spec.getJvmHeapMemorySize());
        assertThat(MemorySize.parse(configs.get("-XX:MaxMetaspaceSize=")))
                .isEqualTo(spec.getJvmMetaspaceSize());
        assertThat(MemorySize.parse(configs.get("-XX:MaxDirectMemorySize=")))
                .isEqualTo(spec.getJvmDirectMemorySize());
    }

    @Test
    void testGenerateJvmParametersWithoutDirectMemoryLimit() {
        ProcessMemorySpec spec = JvmArgTestingProcessMemorySpec.generate();
        String jvmParamsStr = ProcessMemoryUtils.generateJvmParametersStr(spec, false);
        Map<String, String> configs = ConfigurationUtils.parseJvmArgString(jvmParamsStr);

        assertThat(configs).hasSize(3);
        assertThat(MemorySize.parse(configs.get("-Xmx"))).isEqualTo(spec.getJvmHeapMemorySize());
        assertThat(MemorySize.parse(configs.get("-Xms"))).isEqualTo(spec.getJvmHeapMemorySize());
        assertThat(MemorySize.parse(configs.get("-XX:MaxMetaspaceSize=")))
                .isEqualTo(spec.getJvmMetaspaceSize());
        assertThat(configs.containsKey("-XX:MaxDirectMemorySize=")).isFalse();
    }

    @Test
    void testConfigTotalFlinkMemory() {
        MemorySize totalFlinkMemorySize = MemorySize.parse("1g");

        Configuration conf = new Configuration();
        conf.set(options.getTotalFlinkMemoryOption(), totalFlinkMemorySize);

        T processSpec = processSpecFromConfig(conf);
        assertThat(processSpec.getTotalFlinkMemorySize()).isEqualTo(totalFlinkMemorySize);
    }

    @Test
    void testConfigTotalProcessMemorySize() {
        MemorySize totalProcessMemorySize = MemorySize.parse("2g");

        Configuration conf = new Configuration();
        conf.set(options.getTotalProcessMemoryOption(), totalProcessMemorySize);

        T processSpec = processSpecFromConfig(conf);
        assertThat(processSpec.getTotalProcessMemorySize()).isEqualTo(totalProcessMemorySize);
    }

    @Test
    void testExceptionShouldContainRequiredConfigOptions() {
        try {
            processSpecFromConfig(new Configuration());
        } catch (IllegalConfigurationException e) {
            options.getRequiredFineGrainedOptions()
                    .forEach(option -> assertThat(e).hasMessageContaining(option.key()));
            assertThat(e)
                    .hasMessageContaining(options.getTotalFlinkMemoryOption().key())
                    .hasMessageContaining(options.getTotalProcessMemoryOption().key());
        }
    }

    @Test
    void testDerivedTotalProcessMemoryGreaterThanConfiguredFailureWithFineGrainedOptions() {
        Configuration conf = getConfigurationWithJvmMetaspaceAndTotalFlinkMemory(100, 200);
        // Total Flink memory + JVM Metaspace > Total Process Memory (no space for JVM overhead)
        MemorySize totalFlinkMemorySize = MemorySize.ofMebiBytes(150);
        configWithFineGrainedOptions(conf, totalFlinkMemorySize);
        validateFail(conf);
    }

    @Test
    void testDerivedTotalProcessMemoryGreaterThanConfiguredFailureWithTotalFlinkMemory() {
        Configuration conf = getConfigurationWithJvmMetaspaceAndTotalFlinkMemory(100, 200);
        // Total Flink memory + JVM Metaspace > Total Process Memory (no space for JVM overhead)
        MemorySize totalFlinkMemorySize = MemorySize.ofMebiBytes(150);
        conf.set(options.getTotalFlinkMemoryOption(), totalFlinkMemorySize);
        validateFail(conf);
    }

    private Configuration getConfigurationWithJvmMetaspaceAndTotalFlinkMemory(
            long jvmMetaspaceSizeMb, long totalProcessMemorySizeMb) {
        MemorySize jvmMetaspaceSize = MemorySize.ofMebiBytes(jvmMetaspaceSizeMb);
        MemorySize totalProcessMemorySize = MemorySize.ofMebiBytes(totalProcessMemorySizeMb);
        Configuration conf = new Configuration();
        conf.set(options.getJvmOptions().getJvmMetaspaceOption(), jvmMetaspaceSize);
        conf.set(options.getTotalProcessMemoryOption(), totalProcessMemorySize);
        return conf;
    }

    @Test
    void testConfigJvmMetaspaceSize() {
        MemorySize jvmMetaspaceSize = MemorySize.parse("50m");

        Configuration conf = new Configuration();
        conf.set(options.getJvmOptions().getJvmMetaspaceOption(), jvmMetaspaceSize);

        validateInAllConfigurations(
                conf,
                processSpec ->
                        assertThat(processSpec.getJvmMetaspaceSize()).isEqualTo(jvmMetaspaceSize));
    }

    @Test
    void testConfigJvmOverheadRange() {
        MemorySize minSize = MemorySize.parse("50m");
        MemorySize maxSize = MemorySize.parse("200m");

        Configuration conf = new Configuration();
        conf.set(options.getJvmOptions().getJvmOverheadMax(), maxSize);
        conf.set(options.getJvmOptions().getJvmOverheadMin(), minSize);

        validateInAllConfigurations(
                conf,
                JobManagerProcessSpec -> {
                    assertThat(JobManagerProcessSpec.getJvmOverheadSize().getBytes())
                            .isGreaterThanOrEqualTo(minSize.getBytes());
                    assertThat(JobManagerProcessSpec.getJvmOverheadSize().getBytes())
                            .isLessThanOrEqualTo(maxSize.getBytes());
                });
    }

    @Test
    void testConfigJvmOverheadRangeFailure() {
        MemorySize minSize = MemorySize.parse("200m");
        MemorySize maxSize = MemorySize.parse("50m");

        Configuration conf = new Configuration();
        conf.set(options.getJvmOptions().getJvmOverheadMax(), maxSize);
        conf.set(options.getJvmOptions().getJvmOverheadMin(), minSize);

        validateFailInAllConfigurations(conf);
    }

    @Test
    void testConfigJvmOverheadFraction() {
        MemorySize minSize = MemorySize.ZERO;
        MemorySize maxSize = MemorySize.parse("1t");
        @SuppressWarnings("MagicNumber")
        float fraction = 0.2f;

        Configuration conf = new Configuration();
        conf.set(options.getJvmOptions().getJvmOverheadMax(), maxSize);
        conf.set(options.getJvmOptions().getJvmOverheadMin(), minSize);
        conf.setFloat(options.getJvmOptions().getJvmOverheadFraction(), fraction);

        validateInAllConfigurations(
                conf,
                jobManagerProcessSpec ->
                        assertThat(jobManagerProcessSpec.getJvmOverheadSize())
                                .isEqualTo(
                                        jobManagerProcessSpec
                                                .getTotalProcessMemorySize()
                                                .multiply(fraction)));
    }

    @Test
    void testConfigJvmOverheadFractionFailureNegative() {
        Configuration conf = new Configuration();
        //noinspection MagicNumber
        conf.setFloat(options.getJvmOptions().getJvmOverheadFraction(), -0.1f);
        validateFailInAllConfigurations(conf);
    }

    @Test
    void testConfigJvmOverheadFractionFailureNoLessThanOne() {
        Configuration conf = new Configuration();
        conf.setFloat(options.getJvmOptions().getJvmOverheadFraction(), 1.0f);
        validateFailInAllConfigurations(conf);
    }

    @Test
    void testConfigJvmOverheadDeriveFromProcessAndFlinkMemorySize() {
        Configuration conf = new Configuration();
        conf.set(options.getTotalProcessMemoryOption(), MemorySize.parse("1000m"));
        conf.set(options.getTotalFlinkMemoryOption(), MemorySize.parse("800m"));
        conf.set(options.getJvmOptions().getJvmMetaspaceOption(), MemorySize.parse("100m"));
        conf.set(options.getJvmOptions().getJvmOverheadMin(), MemorySize.parse("50m"));
        conf.set(options.getJvmOptions().getJvmOverheadMax(), MemorySize.parse("200m"));
        //noinspection MagicNumber
        conf.set(options.getJvmOptions().getJvmOverheadFraction(), 0.5f);

        T jobManagerProcessSpec = processSpecFromConfig(conf);
        assertThat(jobManagerProcessSpec.getJvmOverheadSize()).isEqualTo(MemorySize.parse("100m"));
    }

    @Test
    void testConfigJvmOverheadDeriveFromProcessAndFlinkMemorySizeFailure() {
        Configuration conf = new Configuration();
        conf.set(options.getTotalProcessMemoryOption(), MemorySize.parse("1000m"));
        conf.set(options.getTotalFlinkMemoryOption(), MemorySize.parse("800m"));
        conf.set(options.getJvmOptions().getJvmMetaspaceOption(), MemorySize.parse("100m"));
        conf.set(options.getJvmOptions().getJvmOverheadMin(), MemorySize.parse("150m"));
        conf.set(options.getJvmOptions().getJvmOverheadMax(), MemorySize.parse("200m"));
        //noinspection MagicNumber
        conf.set(options.getJvmOptions().getJvmOverheadFraction(), 0.5f);

        validateFail(conf);
    }

    @Test
    void testConfigLegacyHeapSize() {
        MemorySize legacyHeapSize = MemorySize.parse("1g");

        Configuration conf = new Configuration();
        conf.set(legacyMemoryOptions.getHeap(), legacyHeapSize);

        testConfigLegacyHeapMemory(conf, legacyHeapSize);
    }

    @Test
    void testConfigLegacyHeapMB() {
        MemorySize jvmHeapSize = MemorySize.parse("1g");

        Configuration conf = new Configuration();
        conf.set(legacyMemoryOptions.getHeapMb(), jvmHeapSize.getMebiBytes());

        testConfigLegacyHeapMemory(conf, jvmHeapSize);
    }

    @Test
    void testConfigLegacyHeapEnv() {
        MemorySize jvmHeapSize = MemorySize.parse("1g");

        Map<String, String> env = new HashMap<>();
        env.put(legacyMemoryOptions.getEnvVar(), "1g");
        CommonTestUtils.setEnv(env);

        testConfigLegacyHeapMemory(new Configuration(), jvmHeapSize);
    }

    @Test
    void testConfigBothNewOptionAndLegacyHeapSize() {
        MemorySize newOptionValue = MemorySize.parse("1g");
        MemorySize legacyHeapSize = MemorySize.parse("2g");

        Configuration conf = new Configuration();
        conf.set(getNewOptionForLegacyHeapOption(), newOptionValue);
        conf.set(legacyMemoryOptions.getHeap(), legacyHeapSize);

        testConfigLegacyHeapMemory(conf, newOptionValue);
    }

    private void testConfigLegacyHeapMemory(Configuration configuration, MemorySize expected) {
        MemorySize newOptionValue =
                getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(configuration)
                        .get(getNewOptionForLegacyHeapOption());
        assertThat(newOptionValue).isEqualTo(expected);
    }

    @Test
    void testConfigTotalProcessMemoryAddUpFailure() {
        MemorySize totalProcessMemory = MemorySize.parse("699m");
        MemorySize totalFlinkMemory = MemorySize.parse("500m");
        MemorySize jvmMetaspace = MemorySize.parse("100m");
        MemorySize jvmOverhead = MemorySize.parse("100m");

        Configuration conf = new Configuration();
        conf.set(options.getTotalProcessMemoryOption(), totalProcessMemory);
        conf.set(options.getTotalFlinkMemoryOption(), totalFlinkMemory);
        conf.set(options.getJvmOptions().getJvmMetaspaceOption(), jvmMetaspace);
        conf.set(options.getJvmOptions().getJvmOverheadMin(), jvmOverhead);
        conf.set(options.getJvmOptions().getJvmOverheadMax(), jvmOverhead);

        validateFail(conf);
    }

    protected abstract void validateInAllConfigurations(
            Configuration customConfig, Consumer<T> validateFunc);

    protected abstract void validateFailInAllConfigurations(Configuration customConfig);

    protected abstract void validateFail(Configuration config);

    protected abstract T processSpecFromConfig(Configuration config);

    protected abstract Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
            Configuration config);

    protected abstract void configWithFineGrainedOptions(
            Configuration configuration, MemorySize totalFlinkMemorySize);

    protected ConfigOption<MemorySize> getNewOptionForLegacyHeapOption() {
        return newOptionForLegacyHeapOption;
    }

    private static class JvmArgTestingProcessMemorySpec implements ProcessMemorySpec {
        private static final long serialVersionUID = 2863985135320165745L;

        private final MemorySize heap;
        private final MemorySize directMemory;
        private final MemorySize metaspace;

        private JvmArgTestingProcessMemorySpec(
                MemorySize heap, MemorySize directMemory, MemorySize metaspace) {
            this.heap = heap;
            this.directMemory = directMemory;
            this.metaspace = metaspace;
        }

        @Override
        public MemorySize getJvmHeapMemorySize() {
            return heap;
        }

        @Override
        public MemorySize getJvmDirectMemorySize() {
            return directMemory;
        }

        @Override
        public MemorySize getJvmMetaspaceSize() {
            return metaspace;
        }

        @Override
        public MemorySize getJvmOverheadSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemorySize getTotalFlinkMemorySize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemorySize getTotalProcessMemorySize() {
            throw new UnsupportedOperationException();
        }

        public static JvmArgTestingProcessMemorySpec generate() {
            return new JvmArgTestingProcessMemorySpec(
                    MemorySize.ofMebiBytes(1),
                    MemorySize.ofMebiBytes(2),
                    MemorySize.ofMebiBytes(3));
        }
    }
}
