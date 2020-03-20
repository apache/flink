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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common utils to parse JVM process memory configuration for JM or TM.
 */
public abstract class ProcessMemoryUtils
		<S extends ProcessMemoryUtils.MemoryProcessSpec, IM extends ProcessMemoryUtils.FlinkInternalMemory> {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessMemoryUtils.class);

	private final List<ConfigOption<MemorySize>> internalMemoryOptions;
	private final ConfigOption<MemorySize> totalFlinkOption;
	private final ConfigOption<MemorySize> totalProcessOption;
	private final JvmMetaspaceAndOverheadOptions jvmOptions;
	private final LegacyHeapOptions legacyHeapOptions;

	protected ProcessMemoryUtils(
			List<ConfigOption<MemorySize>> internalMemoryOptions,
			ConfigOption<MemorySize> totalFlinkOption,
			ConfigOption<MemorySize> totalProcessOption,
			JvmMetaspaceAndOverheadOptions jvmOptions,
			LegacyHeapOptions legacyHeapOptions) {
		this.internalMemoryOptions = checkNotNull(internalMemoryOptions);
		this.totalFlinkOption = checkNotNull(totalFlinkOption);
		this.totalProcessOption = checkNotNull(totalProcessOption);
		this.jvmOptions = checkNotNull(jvmOptions);
		this.legacyHeapOptions = legacyHeapOptions;
	}

	public S memoryProcessSpecFromConfig(Configuration config) {
		if (internalMemoryOptions.stream().allMatch(config::contains)) {
			// all internal memory options are configured, use these to derive total Flink and process memory
			return deriveProcessSpecWithExplicitInternalMemory(config);
		} else if (config.contains(totalFlinkOption)) {
			// internal memory options are not configured, total Flink memory is configured,
			// derive from total flink memory
			return deriveProcessSpecWithTotalFlinkMemory(config);
		} else if (config.contains(totalProcessOption)) {
			// total Flink memory is not configured, total process memory is configured,
			// derive from total process memory
			return deriveProcessSpecWithTotalProcessMemory(config);
		}
		return failIfMandatoryOptionsNotConfigured();
	}

	private S deriveProcessSpecWithExplicitInternalMemory(Configuration config) {
		IM flinkInternalMemory = deriveTotalFlinkMemoryFromInternalMemory(config);
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
			config,
			flinkInternalMemory.getTotalFlinkMemorySize());
		return createMemoryProcessSpec(config, flinkInternalMemory, jvmMetaspaceAndOverhead);
	}

	private S deriveProcessSpecWithTotalFlinkMemory(Configuration config) {
		MemorySize totalFlinkMemorySize = getMemorySizeFromConfig(config, totalFlinkOption);
		IM internalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(config, totalFlinkMemorySize);
		return createMemoryProcessSpec(config, internalMemory, jvmMetaspaceAndOverhead);
	}

	private S deriveProcessSpecWithTotalProcessMemory(Configuration config) {
		MemorySize totalProcessMemorySize = getMemorySizeFromConfig(config, totalProcessOption);
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =
			deriveJvmMetaspaceAndOverheadWithTotalProcessMemory(config, totalProcessMemorySize);
		MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(
			jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize());
		IM internalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);
		return createMemoryProcessSpec(config, internalMemory, jvmMetaspaceAndOverhead);
	}

	protected abstract IM deriveTotalFlinkMemoryFromInternalMemory(Configuration config);

	protected abstract IM deriveInternalMemoryFromTotalFlinkMemory(Configuration config, MemorySize totalFlinkMemorySize);

	protected abstract S createMemoryProcessSpec(
		Configuration config,
		IM internalMemory,
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead);

	protected abstract S failIfMandatoryOptionsNotConfigured();

	private JvmMetaspaceAndOverhead deriveJvmMetaspaceAndOverheadWithTotalProcessMemory(
			Configuration config,
			MemorySize totalProcessMemorySize) {
		MemorySize jvmMetaspaceSize = getMemorySizeFromConfig(config, jvmOptions.getJvmMetaspaceOption());
		MemorySize jvmOverheadSize = deriveWithFraction(
			"jvm overhead memory",
			totalProcessMemorySize,
			getJvmOverheadRangeFraction(config));
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);

		if (jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize().getBytes() > totalProcessMemorySize.getBytes()) {
			throw new IllegalConfigurationException(
				"Sum of configured JVM Metaspace (" + jvmMetaspaceAndOverhead.getMetaspace().toHumanReadableString()
					+ ") and JVM Overhead (" + jvmMetaspaceAndOverhead.getOverhead().toHumanReadableString()
					+ ") exceed configured Total Process Memory (" + totalProcessMemorySize.toHumanReadableString() + ").");
		}

		return jvmMetaspaceAndOverhead;
	}

	private JvmMetaspaceAndOverhead deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
			Configuration config,
			MemorySize totalFlinkMemorySize) {
		MemorySize jvmMetaspaceSize = getMemorySizeFromConfig(config, jvmOptions.getJvmMetaspaceOption());
		MemorySize totalFlinkAndJvmMetaspaceSize = totalFlinkMemorySize.add(jvmMetaspaceSize);
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead;
		if (config.contains(totalProcessOption)) {
			MemorySize totalProcessMemorySize = getMemorySizeFromConfig(config, totalProcessOption);
			MemorySize jvmOverheadSize = totalProcessMemorySize.subtract(totalFlinkAndJvmMetaspaceSize);
			sanityCheckJvmOverhead(config, jvmOverheadSize, totalProcessMemorySize);
			jvmMetaspaceAndOverhead = new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);
		} else {
			MemorySize jvmOverheadSize = deriveWithInverseFraction(
				"jvm overhead memory",
				totalFlinkAndJvmMetaspaceSize,
				getJvmOverheadRangeFraction(config));
			jvmMetaspaceAndOverhead = new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);
			sanityCheckTotalProcessMemory(config, totalFlinkMemorySize, jvmMetaspaceAndOverhead);
		}
		return jvmMetaspaceAndOverhead;
	}

	private void sanityCheckJvmOverhead(
			Configuration config,
			MemorySize derivedJvmOverheadSize,
			MemorySize totalProcessMemorySize) {
		RangeFraction jvmOverheadRangeFraction = getJvmOverheadRangeFraction(config);
		if (derivedJvmOverheadSize.getBytes() > jvmOverheadRangeFraction.getMaxSize().getBytes() ||
			derivedJvmOverheadSize.getBytes() < jvmOverheadRangeFraction.getMinSize().getBytes()) {
			throw new IllegalConfigurationException("Derived JVM Overhead size ("
				+ derivedJvmOverheadSize.toHumanReadableString() + ") is not in configured JVM Overhead range ["
				+ jvmOverheadRangeFraction.getMinSize().toHumanReadableString() + ", "
				+ jvmOverheadRangeFraction.getMaxSize().toHumanReadableString() + "].");
		}
		if (config.contains(jvmOptions.getJvmOverheadFraction()) &&
			!derivedJvmOverheadSize.equals(totalProcessMemorySize.multiply(jvmOverheadRangeFraction.getFraction()))) {
			LOG.info(
				"The derived JVM Overhead size ({}) does not match " +
					"the configured JVM Overhead fraction ({}) from the configured Total Process Memory size ({}). " +
					"The derived JVM OVerhead size will be used.",
				derivedJvmOverheadSize.toHumanReadableString(),
				jvmOverheadRangeFraction.getFraction(),
				totalProcessMemorySize.toHumanReadableString());
		}
	}

	private void sanityCheckTotalProcessMemory(
			Configuration config,
			MemorySize totalFlinkMemory,
			JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		MemorySize derivedTotalProcessMemorySize =
			totalFlinkMemory.add(jvmMetaspaceAndOverhead.getMetaspace()).add(jvmMetaspaceAndOverhead.getOverhead());
		if (config.contains(totalProcessOption)) {
			MemorySize configuredTotalProcessMemorySize = getMemorySizeFromConfig(config, totalProcessOption);
			if (!configuredTotalProcessMemorySize.equals(derivedTotalProcessMemorySize)) {
				throw new IllegalConfigurationException(String.format(
					"Configured and Derived memory sizes (total %s) do not add up to the configured Total Process " +
						"Memory size (%s). Configured and Derived memory sizes are: Total Flink Memory (%s), " +
						"JVM Metaspace (%s), JVM Overhead (%s).",
					derivedTotalProcessMemorySize.toHumanReadableString(),
					configuredTotalProcessMemorySize.toHumanReadableString(),
					totalFlinkMemory.toHumanReadableString(),
					jvmMetaspaceAndOverhead.getMetaspace().toHumanReadableString(),
					jvmMetaspaceAndOverhead.getOverhead().toHumanReadableString()));
			}
		}
	}

	private RangeFraction getJvmOverheadRangeFraction(Configuration config) {
		MemorySize minSize = getMemorySizeFromConfig(config, jvmOptions.getJvmOverheadMin());
		MemorySize maxSize = getMemorySizeFromConfig(config, jvmOptions.getJvmOverheadMax());
		return getRangeFraction(minSize, maxSize, jvmOptions.getJvmOverheadFraction(), config);
	}

	public Configuration getConfWithLegacyHeapSizeMappedToNewConfigOption(
		Configuration configuration,
		ConfigOption<MemorySize> configOption) {
		if (configuration.contains(configOption)) {
			return configuration;
		}
		return getLegacyTaskManagerHeapMemoryIfExplicitlyConfigured(configuration)
			.map(legacyHeapSize -> {
				Configuration copiedConfig = new Configuration(configuration);
				copiedConfig.set(configOption, legacyHeapSize);
				LOG.info(
					"'{}' is not specified, use the configured deprecated task manager heap value ({}) for it.",
					configOption.key(),
					legacyHeapSize.toHumanReadableString());
				return copiedConfig;
			}).orElse(configuration);
	}

	private Optional<MemorySize> getLegacyTaskManagerHeapMemoryIfExplicitlyConfigured(Configuration configuration) {
		@SuppressWarnings("CallToSystemGetenv")
		String totalProcessEnv = System.getenv(legacyHeapOptions.getEnvVar());
		if (totalProcessEnv != null) {
			//noinspection OverlyBroadCatchBlock
			try {
				return Optional.of(MemorySize.parse(totalProcessEnv));
			} catch (Throwable t) {
				throw new IllegalConfigurationException(
					"Cannot read total process memory size from environment variable value " + totalProcessEnv + '.',
					t);
			}
		}

		if (configuration.contains(legacyHeapOptions.getHeap())) {
			return Optional.of(getMemorySizeFromConfig(configuration, legacyHeapOptions.getHeap()));
		}

		if (configuration.contains(legacyHeapOptions.getHeapMb())) {
			final long legacyHeapMemoryMB = configuration.getInteger(legacyHeapOptions.getHeapMb());
			if (legacyHeapMemoryMB < 0) {
				throw new IllegalConfigurationException(
					"Configured total process memory size (" + legacyHeapMemoryMB + "MB) must not be less than 0.");
			}
			//noinspection MagicNumber
			return Optional.of(new MemorySize(legacyHeapMemoryMB << 20)); // megabytes to bytes;
		}

		return Optional.empty();
	}

	protected static MemorySize getMemorySizeFromConfig(Configuration config, ConfigOption<MemorySize> option) {
		try {
			return Preconditions.checkNotNull(config.get(option), "The memory option is not set and has no default value.");
		} catch (Throwable t) {
			throw new IllegalConfigurationException("Cannot read memory size from config option '" + option.key() + "'.", t);
		}
	}

	protected static RangeFraction getRangeFraction(
			MemorySize minSize,
			MemorySize maxSize,
			ConfigOption<Float> fractionOption,
			Configuration config) {
		double fraction = config.getFloat(fractionOption);
		try {
			return new RangeFraction(minSize, maxSize, fraction);
		} catch (IllegalArgumentException e) {
			throw new IllegalConfigurationException(
				String.format(
					"Inconsistently configured %s (%s) and its min (%s), max (%s) value",
					fractionOption,
					fraction,
					minSize.toHumanReadableString(),
					maxSize.toHumanReadableString()),
				e);
		}
	}

	protected static MemorySize deriveWithFraction(String memoryDescription, MemorySize base, RangeFraction rangeFraction) {
		MemorySize relative = base.multiply(rangeFraction.getFraction());
		return capToMinMax(memoryDescription, relative, rangeFraction);
	}

	protected static MemorySize deriveWithInverseFraction(String memoryDescription, MemorySize base, RangeFraction rangeFraction) {
		checkArgument(rangeFraction.getFraction() < 1);
		MemorySize relative = base.multiply(rangeFraction.getFraction() / (1 - rangeFraction.getFraction()));
		return capToMinMax(memoryDescription, relative, rangeFraction);
	}

	private static MemorySize capToMinMax(
			String memoryDescription,
			MemorySize relative,
			RangeFraction rangeFraction) {
		long size = relative.getBytes();
		if (size > rangeFraction.getMaxSize().getBytes()) {
			LOG.info(
				"The derived from fraction {} ({}) is greater than its max value {}, max value will be used instead",
				memoryDescription,
				relative.toHumanReadableString(),
				rangeFraction.getMaxSize().toHumanReadableString());
			size = rangeFraction.getMaxSize().getBytes();
		} else if (size < rangeFraction.getMinSize().getBytes()) {
			LOG.info(
				"The derived from fraction {} ({}) is less than its min value {}, min value will be used instead",
				memoryDescription,
				relative.toHumanReadableString(),
				rangeFraction.getMinSize().toHumanReadableString());
			size = rangeFraction.getMinSize().getBytes();
		}
		return new MemorySize(size);
	}

	public static String generateJvmParametersStr(MemoryProcessSpec processSpec) {
		return "-Xmx" + processSpec.getJvmHeapMemorySize().getBytes()
			+ " -Xms" + processSpec.getJvmHeapMemorySize().getBytes()
			+ " -XX:MaxDirectMemorySize=" + processSpec.getJvmDirectMemorySize().getBytes()
			+ " -XX:MaxMetaspaceSize=" + processSpec.getJvmMetaspaceSize().getBytes();
	}

	/**
	 * Memory components which constitute the Total Flink Memory.
	 */
	@FunctionalInterface
	public interface FlinkInternalMemory {
		MemorySize getTotalFlinkMemorySize();
	}

	/**
	 * Common interface for Flink JVM process memory components.
	 */
	public interface MemoryProcessSpec extends Serializable {
		MemorySize getJvmHeapMemorySize();

		MemorySize getJvmDirectMemorySize();

		MemorySize getJvmMetaspaceSize();

		MemorySize getJvmOverheadSize();

		MemorySize getTotalFlinkMemorySize();

		MemorySize getTotalProcessMemorySize();
	}

	/**
	 * Options to calculate JVM Metaspace and Overhead.
	 */
	public static class JvmMetaspaceAndOverheadOptions {
		private final ConfigOption<MemorySize> jvmMetaspaceOption;
		private final ConfigOption<MemorySize> jvmOverheadMin;
		private final ConfigOption<MemorySize> jvmOverheadMax;
		private final ConfigOption<Float> jvmOverheadFraction;

		public JvmMetaspaceAndOverheadOptions(
				ConfigOption<MemorySize> jvmMetaspaceOption,
				ConfigOption<MemorySize> jvmOverheadMin,
				ConfigOption<MemorySize> jvmOverheadMax,
				ConfigOption<Float> jvmOverheadFraction) {
			this.jvmMetaspaceOption = jvmMetaspaceOption;
			this.jvmOverheadMin = jvmOverheadMin;
			this.jvmOverheadMax = jvmOverheadMax;
			this.jvmOverheadFraction = jvmOverheadFraction;
		}

		ConfigOption<MemorySize> getJvmMetaspaceOption() {
			return jvmMetaspaceOption;
		}

		ConfigOption<MemorySize> getJvmOverheadMin() {
			return jvmOverheadMin;
		}

		ConfigOption<MemorySize> getJvmOverheadMax() {
			return jvmOverheadMax;
		}

		ConfigOption<Float> getJvmOverheadFraction() {
			return jvmOverheadFraction;
		}
	}

	/**
	 * JVM metaspace and overhead memory sizes.
	 */
	public static class JvmMetaspaceAndOverhead {
		private final MemorySize metaspace;
		private final MemorySize overhead;

		JvmMetaspaceAndOverhead(MemorySize jvmMetaspace, MemorySize jvmOverhead) {
			this.metaspace = checkNotNull(jvmMetaspace);
			this.overhead = checkNotNull(jvmOverhead);
		}

		MemorySize getTotalJvmMetaspaceAndOverheadSize() {
			return getMetaspace().add(getOverhead());
		}

		public MemorySize getMetaspace() {
			return metaspace;
		}

		public MemorySize getOverhead() {
			return overhead;
		}
	}

	/**
	 * Range and fraction of a memory component, which is a capped fraction of another component.
	 */
	public static class RangeFraction {
		private final MemorySize minSize;
		private final MemorySize maxSize;
		private final double fraction;

		RangeFraction(MemorySize minSize, MemorySize maxSize, double fraction) {
			this.minSize = minSize;
			this.maxSize = maxSize;
			this.fraction = fraction;
			checkArgument(minSize.getBytes() <= maxSize.getBytes(), "min value must be less or equal to max value");
			checkArgument(fraction >= 0 && fraction < 1, "fraction must be in range [0, 1)");
		}

		public MemorySize getMinSize() {
			return minSize;
		}

		public MemorySize getMaxSize() {
			return maxSize;
		}

		public double getFraction() {
			return fraction;
		}
	}

	/**
	 * Legacy JVM heap/process memory options to fallback to new options.
	 */
	public static class LegacyHeapOptions {
		private final String envVar;
		private final ConfigOption<MemorySize> heap;
		private final ConfigOption<Integer> heapMb;

		public LegacyHeapOptions(String envVar, ConfigOption<MemorySize> heap, ConfigOption<Integer> heapMb) {
			this.envVar = envVar;
			this.heap = heap;
			this.heapMb = heapMb;
		}

		String getEnvVar() {
			return envVar;
		}

		ConfigOption<MemorySize> getHeap() {
			return heap;
		}

		ConfigOption<Integer> getHeapMb() {
			return heapMb;
		}
	}
}
