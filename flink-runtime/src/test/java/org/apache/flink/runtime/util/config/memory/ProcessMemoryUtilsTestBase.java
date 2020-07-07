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
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * Base test suite for JM/TM memory calculations to test common methods in {@link ProcessMemoryUtils}.
 */
@SuppressWarnings("AbstractClassExtendsConcreteClass")
public abstract class ProcessMemoryUtilsTestBase<T extends ProcessMemorySpec> extends TestLogger {
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
	public void testGenerateJvmParameters() {
		ProcessMemorySpec spec = JvmArgTestingProcessMemorySpec.generate();
		String jvmParamsStr = ProcessMemoryUtils.generateJvmParametersStr(spec, true);
		Map<String, String> configs = ConfigurationUtils.parseJvmArgString(jvmParamsStr);

		assertThat(configs.size(), is(4));
		assertThat(MemorySize.parse(configs.get("-Xmx")), is(spec.getJvmHeapMemorySize()));
		assertThat(MemorySize.parse(configs.get("-Xms")), is(spec.getJvmHeapMemorySize()));
		assertThat(MemorySize.parse(configs.get("-XX:MaxMetaspaceSize=")), is(spec.getJvmMetaspaceSize()));
		assertThat(MemorySize.parse(configs.get("-XX:MaxDirectMemorySize=")), is(spec.getJvmDirectMemorySize()));
	}

	@Test
	public void testGenerateJvmParametersWithoutDirectMemoryLimit() {
		ProcessMemorySpec spec = JvmArgTestingProcessMemorySpec.generate();
		String jvmParamsStr = ProcessMemoryUtils.generateJvmParametersStr(spec, false);
		Map<String, String> configs = ConfigurationUtils.parseJvmArgString(jvmParamsStr);

		assertThat(configs.size(), is(3));
		assertThat(MemorySize.parse(configs.get("-Xmx")), is(spec.getJvmHeapMemorySize()));
		assertThat(MemorySize.parse(configs.get("-Xms")), is(spec.getJvmHeapMemorySize()));
		assertThat(MemorySize.parse(configs.get("-XX:MaxMetaspaceSize=")), is(spec.getJvmMetaspaceSize()));
		assertThat(configs.containsKey("-XX:MaxDirectMemorySize="), is(false));
	}

	@Test
	public void testConfigTotalFlinkMemory() {
		MemorySize totalFlinkMemorySize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.set(options.getTotalFlinkMemoryOption(), totalFlinkMemorySize);

		T processSpec = processSpecFromConfig(conf);
		assertThat(processSpec.getTotalFlinkMemorySize(), is(totalFlinkMemorySize));
	}

	@Test
	public void testConfigTotalProcessMemorySize() {
		MemorySize totalProcessMemorySize = MemorySize.parse("2g");

		Configuration conf = new Configuration();
		conf.set(options.getTotalProcessMemoryOption(), totalProcessMemorySize);

		T processSpec = processSpecFromConfig(conf);
		assertThat(processSpec.getTotalProcessMemorySize(), is(totalProcessMemorySize));
	}

	@Test
	public void testExceptionShouldContainRequiredConfigOptions() {
		try {
			processSpecFromConfig(new Configuration());
		} catch (IllegalConfigurationException e) {
			options.getRequiredFineGrainedOptions().forEach(option -> assertThat(e.getMessage(), containsString(option.key())));
			assertThat(e.getMessage(), containsString(options.getTotalFlinkMemoryOption().key()));
			assertThat(e.getMessage(), containsString(options.getTotalProcessMemoryOption().key()));
		}
	}

	@Test
	public void testDerivedTotalProcessMemoryGreaterThanConfiguredFailureWithFineGrainedOptions() {
		Configuration conf = getConfigurationWithJvmMetaspaceAndTotalFlinkMemory(100, 200);
		// Total Flink memory + JVM Metaspace > Total Process Memory (no space for JVM overhead)
		MemorySize totalFlinkMemorySize = MemorySize.ofMebiBytes(150);
		configWithFineGrainedOptions(conf, totalFlinkMemorySize);
		validateFail(conf);
	}

	@Test
	public void testDerivedTotalProcessMemoryGreaterThanConfiguredFailureWithTotalFlinkMemory() {
		Configuration conf = getConfigurationWithJvmMetaspaceAndTotalFlinkMemory(100, 200);
		// Total Flink memory + JVM Metaspace > Total Process Memory (no space for JVM overhead)
		MemorySize totalFlinkMemorySize = MemorySize.ofMebiBytes(150);
		conf.set(options.getTotalFlinkMemoryOption(), totalFlinkMemorySize);
		validateFail(conf);
	}

	private Configuration getConfigurationWithJvmMetaspaceAndTotalFlinkMemory(
			long jvmMetaspaceSizeMb,
			long totalProcessMemorySizeMb) {
		MemorySize jvmMetaspaceSize = MemorySize.ofMebiBytes(jvmMetaspaceSizeMb);
		MemorySize totalProcessMemorySize = MemorySize.ofMebiBytes(totalProcessMemorySizeMb);
		Configuration conf = new Configuration();
		conf.set(options.getJvmOptions().getJvmMetaspaceOption(), jvmMetaspaceSize);
		conf.set(options.getTotalProcessMemoryOption(), totalProcessMemorySize);
		return conf;
	}

	@Test
	public void testConfigJvmMetaspaceSize() {
		MemorySize jvmMetaspaceSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(options.getJvmOptions().getJvmMetaspaceOption(), jvmMetaspaceSize);

		validateInAllConfigurations(
			conf,
			processSpec -> assertThat(processSpec.getJvmMetaspaceSize(), is(jvmMetaspaceSize)));
	}

	@Test
	public void testConfigJvmOverheadRange() {
		MemorySize minSize = MemorySize.parse("50m");
		MemorySize maxSize = MemorySize.parse("200m");

		Configuration conf = new Configuration();
		conf.set(options.getJvmOptions().getJvmOverheadMax(), maxSize);
		conf.set(options.getJvmOptions().getJvmOverheadMin(), minSize);

		validateInAllConfigurations(conf, JobManagerProcessSpec -> {
			assertThat(JobManagerProcessSpec.getJvmOverheadSize().getBytes(),
				greaterThanOrEqualTo(minSize.getBytes()));
			assertThat(JobManagerProcessSpec.getJvmOverheadSize().getBytes(), lessThanOrEqualTo(maxSize.getBytes()));
		});
	}

	@Test
	public void testConfigJvmOverheadRangeFailure() {
		MemorySize minSize = MemorySize.parse("200m");
		MemorySize maxSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.set(options.getJvmOptions().getJvmOverheadMax(), maxSize);
		conf.set(options.getJvmOptions().getJvmOverheadMin(), minSize);

		validateFailInAllConfigurations(conf);
	}

	@Test
	public void testConfigJvmOverheadFraction() {
		MemorySize minSize = MemorySize.ZERO;
		MemorySize maxSize = MemorySize.parse("1t");
		@SuppressWarnings("MagicNumber") float fraction = 0.2f;

		Configuration conf = new Configuration();
		conf.set(options.getJvmOptions().getJvmOverheadMax(), maxSize);
		conf.set(options.getJvmOptions().getJvmOverheadMin(), minSize);
		conf.setFloat(options.getJvmOptions().getJvmOverheadFraction(), fraction);

		validateInAllConfigurations(
			conf,
			jobManagerProcessSpec -> assertThat(
				jobManagerProcessSpec.getJvmOverheadSize(),
				is(jobManagerProcessSpec.getTotalProcessMemorySize().multiply(fraction))));
	}

	@Test
	public void testConfigJvmOverheadFractionFailureNegative() {
		Configuration conf = new Configuration();
		//noinspection MagicNumber
		conf.setFloat(options.getJvmOptions().getJvmOverheadFraction(), -0.1f);
		validateFailInAllConfigurations(conf);
	}

	@Test
	public void testConfigJvmOverheadFractionFailureNoLessThanOne() {
		Configuration conf = new Configuration();
		conf.setFloat(options.getJvmOptions().getJvmOverheadFraction(), 1.0f);
		validateFailInAllConfigurations(conf);
	}

	@Test
	public void testConfigJvmOverheadDeriveFromProcessAndFlinkMemorySize() {
		Configuration conf = new Configuration();
		conf.set(options.getTotalProcessMemoryOption(), MemorySize.parse("1000m"));
		conf.set(options.getTotalFlinkMemoryOption(), MemorySize.parse("800m"));
		conf.set(options.getJvmOptions().getJvmMetaspaceOption(), MemorySize.parse("100m"));
		conf.set(options.getJvmOptions().getJvmOverheadMin(), MemorySize.parse("50m"));
		conf.set(options.getJvmOptions().getJvmOverheadMax(), MemorySize.parse("200m"));
		//noinspection MagicNumber
		conf.set(options.getJvmOptions().getJvmOverheadFraction(), 0.5f);

		T jobManagerProcessSpec = processSpecFromConfig(conf);
		assertThat(jobManagerProcessSpec.getJvmOverheadSize(), is(MemorySize.parse("100m")));
	}

	@Test
	public void testConfigJvmOverheadDeriveFromProcessAndFlinkMemorySizeFailure() {
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
	public void testConfigLegacyHeapSize() {
		MemorySize legacyHeapSize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.set(legacyMemoryOptions.getHeap(), legacyHeapSize);

		testConfigLegacyHeapMemory(conf, legacyHeapSize);
	}

	@Test
	public void testConfigLegacyHeapMB() {
		MemorySize jvmHeapSize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.set(legacyMemoryOptions.getHeapMb(), jvmHeapSize.getMebiBytes());

		testConfigLegacyHeapMemory(conf, jvmHeapSize);
	}

	@Test
	public void testConfigLegacyHeapEnv() {
		MemorySize jvmHeapSize = MemorySize.parse("1g");

		Map<String, String> env = new HashMap<>();
		env.put(legacyMemoryOptions.getEnvVar(), "1g");
		CommonTestUtils.setEnv(env);

		testConfigLegacyHeapMemory(new Configuration(), jvmHeapSize);
	}

	@Test
	public void testConfigBothNewOptionAndLegacyHeapSize() {
		MemorySize newOptionValue = MemorySize.parse("1g");
		MemorySize legacyHeapSize = MemorySize.parse("2g");

		Configuration conf = new Configuration();
		conf.set(getNewOptionForLegacyHeapOption(), newOptionValue);
		conf.set(legacyMemoryOptions.getHeap(), legacyHeapSize);

		testConfigLegacyHeapMemory(conf, newOptionValue);
	}

	private void testConfigLegacyHeapMemory(Configuration configuration, MemorySize expected) {
		MemorySize newOptionValue = getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(configuration)
			.get(getNewOptionForLegacyHeapOption());
		assertThat(newOptionValue, is(expected));
	}

	@Test
	public void testConfigTotalProcessMemoryAddUpFailure() {
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

	protected abstract void validateInAllConfigurations(Configuration customConfig, Consumer<T> validateFunc);

	protected abstract void validateFailInAllConfigurations(Configuration customConfig);

	protected abstract void validateFail(Configuration config);

	protected abstract T processSpecFromConfig(Configuration config);

	protected abstract Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(Configuration config);

	protected abstract void configWithFineGrainedOptions(Configuration configuration, MemorySize totalFlinkMemorySize);

	protected ConfigOption<MemorySize> getNewOptionForLegacyHeapOption() {
		return newOptionForLegacyHeapOption;
	}

	private static class JvmArgTestingProcessMemorySpec implements ProcessMemorySpec {
		private static final long serialVersionUID = 2863985135320165745L;

		private final MemorySize heap;
		private final MemorySize directMemory;
		private final MemorySize metaspace;

		private JvmArgTestingProcessMemorySpec(MemorySize heap, MemorySize directMemory, MemorySize metaspace) {
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
				MemorySize.ofMebiBytes(3)
			);
		}
	}
}
